package com.foxtel.ingest.lambda;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.foxtel.ingest.aws.dynamodb.DynamoDBManager;
import com.foxtel.ingest.aws.s3.S3Manager;
import com.foxtel.ingest.aws.s3.S3Path;
import com.foxtel.ingest.aws.secrets.SecretsHelper;
import com.foxtel.ingest.constant.IngestIO;
import com.foxtel.ingest.exception.IngestException;
import com.foxtel.ingest.exception.IngestRuntimeException;
import com.foxtel.ingest.json.JsonUtils;
import com.foxtel.ingest.logger.IngestLogger;
import com.foxtel.ingest.util.CommonUtil;
import com.foxtel.ingest.vo.RecordEntityVO;
import com.foxtel.ingest.worker.IngestWorker;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.callbacks.KeyringConfigCallbacks;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchProviderException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Lambda function that ingests data files and inserts them into DynamoDB
 * Author: Josh Passenger <jospas@amazon.com>
 * File ingest to customer table
 * Modified: Prasenjit.Mazumder@foxtel.com.au
 * Logic for delta change detection
 * Audit table entry
 */
@SuppressWarnings("unused")
public class IngestLambda implements RequestHandler<SQSEvent, Void>
{
    /**
     * Static reusable DynamoDB client
     */
    private final DynamoDBManager dynamoDB;

    private final S3Manager s3Manager;

    private final SecretsHelper secretsHelper;

    /**
     * The DynamoDB table to insert into
     */
    private final String customerTableName;

    /**
     * The table that audits ingest requests
     */
    private final String ingestTableName;

    /**
     * The region to use
     */
    private final String region;

    /**
     * Multithreading machinery
     */
    private final int threadCount;

    /**
     * Cached private key contents
     */
    private KeyringConfig keyringConfig = null;

    /**
     * Marker set by workers
     */
    private volatile boolean errored = false;

    private List<IngestWorker> workers = new ArrayList<>();
    private List<Thread> threads = new ArrayList<>();
    private LinkedBlockingQueue<WriteRequest> inbound = null;

    private static final String VERSION = IngestIO.VERSION;
    
    /**
     * Flag for first time ingest
     */
    private boolean isFirstTimeIngest = false;
    
    /**
     * Flags for record compare with current and last processed files record
     */
    private String lastProcessedIngestS3File = "";
    private String currentProcessingIngestS3File = "";
    private String lastCompareStatus = "";
    private RecordEntityVO ingestAuditrecordEntityVO = null;
    private int recordCount = 0;
    private int recordInsertCount = 0;
    private int recordUpdateCount = 0;
    private int recordNoChangeCount = 0;
    private StringBuilder lastLineRead = new StringBuilder();
    private BufferedReader processedFileReader;
    private boolean isHeader = true;
     

    public IngestLambda()
    {
        IngestLogger.info("File Ingest process" + VERSION);
        BouncyGPG.registerProvider();
        this.region = System.getenv(IngestIO.ENV_VARIABLE_REGION);
        this.customerTableName = System.getenv(IngestIO.ENV_VARIABLE_CUSTOMER_TABLE_NAME);
        this.ingestTableName = System.getenv(IngestIO.ENV_VARIABLE_INGEST_TABLE_NAME);
        this.threadCount = Integer.parseInt(System.getenv(IngestIO.ENV_VARIABLE_THREAD_COUNT));
        this.dynamoDB = new DynamoDBManager(region, threadCount);
        this.s3Manager = new S3Manager(region);
        this.secretsHelper = new SecretsHelper(region);
        this.ingestAuditrecordEntityVO = new RecordEntityVO();
    }

    /**
     * Input event handler function that receives the SQS event
     * @param event the event
     * @param context the Lambda context
     * @return returns null
     */
    @Override
    public Void handleRequest(SQSEvent event, Context context) throws IngestRuntimeException
    {
        IngestLogger.setLogger(context.getLogger());
        IngestLogger.info("******* File Ingest process started *******");
        long startTime = Calendar.getInstance().getTimeInMillis();
        for (SQSMessage msg : event.getRecords())
        {
            IngestLogger.info("Received request message from SQS: " + msg.getBody());

            try
            {
                processEvent(msg.getBody());
            }
            catch (IngestException e)
            {
                IngestLogger.error("Failed to process ingest", e);
                throw new IngestRuntimeException("Failed to process ingest", e);
            }
        }

        IngestLogger.info("Ingest procssing is complete. Audit table is getting updated");
        try {
			
        	insertIngestTableWithLatestProcessedFileInfo();
			resetCompareVariable();
			IngestLogger.info("Ingest audit history is updated");
		} catch (IngestException e) {
			IngestLogger.error("Failed to process ingest audit", e);
            throw new IngestRuntimeException("Failed to process audit", e);
		}finally {
			long endTime = Calendar.getInstance().getTimeInMillis();
			IngestLogger.info("******* File Ingest process ended. Time taken"+(endTime-startTime)+" ms *******");
		}
        return null;
    }

	

   
	/**
     * Resets the system for the next ingest
     */
    private void init() throws IngestException
    {
        // Reload the private key and secret each run
        String privateKey = secretsHelper.getSecretString(System.getenv(IngestIO.ENV_VARIABLE_KEY_SECRETS_MANAGER_KEY_ARN));
        String passphrase = secretsHelper.getSecretString(System.getenv(IngestIO.ENV_VARIABLE_KEY_SECRETS_MANAGER_PASSPHRASE_ARN));
        this.keyringConfig = getKeyringConfig(privateKey, passphrase);

        int queueSize = threadCount * 5 * 25;
        inbound = new LinkedBlockingQueue<>(queueSize);
        threads.clear();
        workers.clear();

        for (int i = 0; i < threadCount; i++)
        {
            IngestWorker worker = new IngestWorker(this, customerTableName, dynamoDB, inbound);
            workers.add(worker);

            Thread thread = new Thread(worker);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }

        IngestLogger.info(String.format("Version: %s Threads: %d Queue size: %d Table name: %s",
                VERSION, threadCount, queueSize, customerTableName));
    }

    /**
     * Processes an SQS message string which should
     * contain a serialised S3EventNotification
     * @param message the message to process
     */
    private void processEvent(String message) throws IngestException
    {
        S3EventNotification s3EventNotification = S3EventNotification.parseJson(message);

        for (S3EventNotification.S3EventNotificationRecord record: s3EventNotification.getRecords())
        {
            processRecord(record);
        }
    }

    /**
     * Processes an S3 event record
     * @param record the record to process
     */
    private void processRecord(S3EventNotification.S3EventNotificationRecord record) throws IngestException
    {
        String bucket = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey();
        currentProcessingIngestS3File = key;
        processObject(new S3Path(bucket, key));
    }

    /**
     * Processes an object
     * @param inputPath the S3 input location
     */
    private void processObject(S3Path inputPath) throws IngestException
    {
        if (!inputPath.getKey().endsWith(".csv.gpg"))
        {
            IngestLogger.info("Skipping object which is not an encrypted CSV file: " + inputPath);
            ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_INGEST_ID, IngestIO.STATUS_UN_PROCESSED+"_"+currentProcessingIngestS3File);
            ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_FILE_NAME, currentProcessingIngestS3File);
            return;
        }

        long start = System.currentTimeMillis();

        init();

        IngestLogger.info("Ingesting input encrypted CSV: " + inputPath);

        try (S3Object s3Object = s3Manager.getObject(inputPath))
        {
            IngestLogger.info("Found object length: " + s3Object.getObjectMetadata().getContentLength() + " bytes");
            
            getLatestProcessedIngestFileInfo(); // Get the Latest processed file information from DB
            
            if(!isFirstTimeIngest) 
            {
            	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_INGEST_ID, IngestIO.STATUS_ARCHIVE+"_"+lastProcessedIngestS3File);
            	processedFileReader =  loadBufferReaderFromS3Bucket(inputPath.getBucket(), lastProcessedIngestS3File);
            }
            else 
            {
            	lastCompareStatus = IngestIO.DB_OPERATION_INSERT;
            	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_INGEST_ID, IngestIO.STATUS_CURRENT);
            	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_FILE_NAME, currentProcessingIngestS3File);
            }
             
            processStream(inputPath, s3Object.getObjectContent());
        }
        catch (IngestException e)
        {
        	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_INGEST_ID, IngestIO.STATUS_UN_PROCESSED+"_"+currentProcessingIngestS3File);
            ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_FILE_NAME, currentProcessingIngestS3File);
            throw e;
        }
        catch (Throwable t)
        {
        	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_INGEST_ID, IngestIO.STATUS_UN_PROCESSED+"_"+currentProcessingIngestS3File);
            ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_FILE_NAME, currentProcessingIngestS3File);
            throw new IngestException("Failed to ingest: " + inputPath, t);
        }

        long end = System.currentTimeMillis();

        IngestLogger.info(String.format("Ingestion completed in: %d millis", (end - start)));
    }

    /**
     * Processes an S3 stream, while the input format is a simple non-quoted CSV with a header
     * use a simple parser for performance.
     * This fixes an issue with commons-csv that was causing the stream parse to fail with an MDC
     * error if the CSV was not terminated with extra new line character. Something to do with
     * over-reading the stream perhaps? The same issue arose if I used readLine(), Josh
     */
    private void processStream(final S3Path inputPath, final S3ObjectInputStream in) throws IngestException
    {
        int itemCount = 0;

        // Decrypt the S3 stream and pass to a buffered reader
        try (BufferedReader s3Reader = decryptS3File(in))
        {
            char [] buffer = new char[1];
            String [] columns = null;

            StringBuilder builder = new StringBuilder();

            while (s3Reader.read(buffer) != -1)
            {
                if (buffer[0] == '\n')
                {
                    String line = builder.toString().trim();

                    if (columns == null)
                    {
                        columns = line.split(",");
                    }
                    else
                    {
                    	if (processLine(builder.toString().trim(), columns))
                        {
                            itemCount++;
                        }
                    }

                    builder.setLength(0);
                }

                builder.append(buffer);
            }

            // Processing last record in current file
            if (builder.length() > 0)
            {
                if (processLine(builder.toString().trim(), columns))
            	{
                    itemCount++;
                }
            }

            while (!inbound.isEmpty())
            {
                sleepFor(100L);
            }

            pleaseStop();
            waitForAll();

            if (errored)
            {
                checkForErrors();
            }

            IngestLogger.info("Processed row count: " + itemCount);
            recordCount = itemCount;
            
        }
        catch (IngestException e)
        {
            cleanup();
            throw e;
        }
        catch (Throwable t)
        {
            IngestLogger.error("Failed to process stream for input: " + inputPath, t);
            cleanup();
            throw new IngestException("Failed to  process stream for input: " + inputPath, t);
        }
    }


    
    /**
     * Processes a line into a Dynamo write/update request
     * @param line the line to process
     * @param columns the columns detected
     * @return true if the line was process, false if it was blank
     * @throws IngestException thrown on failure
     * @throws IOException 
     */
    private boolean processLine(String line, String [] columns) throws IngestException, IOException
    {
    	if (StringUtils.isBlank(line))
        {
            return false;
        }

        String [] values = line.split(",");

        if (values.length != columns.length)
        {
            throw new IngestException("Invalid line detected: |" + line + "|");
        }

        String accountId = values[IngestIO.INDEX_ACCOUNTID];
        
        if(!isFirstTimeIngest)
        	compareRecordByAccountIdForDeltaChange(accountId, line);
        if(lastCompareStatus.equals(IngestIO.DB_OPERATION_INSERT) || lastCompareStatus.equals(IngestIO.DB_OPERATION_UPDATE)) 
        {
        	if(lastCompareStatus.equals(IngestIO.DB_OPERATION_INSERT)) 
        	{
        		recordInsertCount++;
        	}
        	else 
        	{
        		recordUpdateCount++;
        	}
        	Map<String, AttributeValue> item = new HashMap<>();
            item.put(IngestIO.COLUMN_ACCOUNTID, new AttributeValue().withS(values[IngestIO.INDEX_ACCOUNTID]));

            if (StringUtils.isNotBlank(values[IngestIO.INDEX_PHONENUMBER1]))
            {
                item.put(IngestIO.COLUMN_PHONENUMBER1, new AttributeValue().withS(values[IngestIO.INDEX_PHONENUMBER1]));
            }

            if (StringUtils.isNotBlank(values[IngestIO.INDEX_PHONENUMBER2]))
            {
                item.put(IngestIO.COLUMN_PHONENUMBER2, new AttributeValue().withS(values[IngestIO.INDEX_PHONENUMBER2]));
            }

            Map<String, String> attributes = new TreeMap<>();

            for (int i = 0; i < columns.length; i++)
            {
                if (StringUtils.isNotBlank(values[i]))
                {
                    attributes.put(columns[i], values[i]);
                }
            }

            item.put(IngestIO.COLUMN_ATTRIBUTES, new AttributeValue().withS(JsonUtils.toJson(attributes)));
            PutRequest putRequest = new PutRequest(item);
            WriteRequest writeRequest = new WriteRequest(putRequest);
            putInbound(writeRequest);
        	
        	
        }
        else if(lastCompareStatus.equals(IngestIO.DB_OPERATION_NOACTION)) 
        {
        	recordNoChangeCount++;
        }
        return true;
    }

    /**
     * Loads the keyring for an exported private key
     * @param privateKey String containing the private key
     * @param passphrase the passphrase for the private key
     * @return the keyring config supplying the private key
     * @throws IngestException thrown on failure
     */
    private KeyringConfig getKeyringConfig(final String privateKey, final String passphrase) throws IngestException
    {
        try
        {
            final InMemoryKeyring keyring = KeyringConfigs.forGpgExportedKeys(KeyringConfigCallbacks.withPassword(passphrase));
            keyring.addSecretKey(privateKey.getBytes(StandardCharsets.US_ASCII));
            return keyring;
        } catch (Throwable t)
        {
            throw new IngestException("Failed to load private key", t);
        }
    }

    private void cleanup()
    {
        cleanupIngestCache();
        resetCompareVariable();
    }

    private void putInbound(WriteRequest writeRequest) throws IngestException
    {
        while (true)
        {
            try
            {
                if (inbound.offer(writeRequest, 10000L, TimeUnit.MILLISECONDS))
                {
                    return;
                }

                if (errored)
                {
                    checkForErrors();
                }

                IngestLogger.info("Queue is full, sleeping");
            }
            catch (InterruptedException ignored)
            {

            }
        }
    }

    private void pleaseStop()
    {
        for (IngestWorker worker: workers)
        {
            worker.pleaseStop();
        }
    }

    private synchronized void waitForAll()
    {
        for (Thread thread : threads)
        {
            while (thread.isAlive())
            {
                sleepFor(50L);
            }
        }
    }

    public void checkForErrors() throws IngestException
    {
        for (IngestWorker worker: workers)
        {
            if (worker.isErrored())
            {
                IngestLogger.error("Processing error detected", worker.getCause());
                throw new IngestException("Processing error detected", worker.getCause());
            }
        }
    }

    /**
     * Sleeps for the requested millis
     * @param millis the number of milliseconds to sleep for
     */
    private void sleepFor(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException ignored)
        {
        }
    }

    /**
     * Marks this as errored
     */
    public void markErrored()
    {
        this.errored = true;
    }

    public void getLatestProcessedIngestFileInfo() throws IngestException
    {
        HashMap<String,AttributeValue> keyAttribute = new HashMap<String,AttributeValue>();
        keyAttribute.put(IngestIO.COLUMN_INGEST_ID, new AttributeValue(IngestIO.STATUS_CURRENT));
        
        try {
        	GetItemRequest getItemRequest =  new GetItemRequest()
                    .withKey(keyAttribute)
                    .withTableName(ingestTableName);
            
            Map<String,AttributeValue> returned_item = dynamoDB.getItem(ingestTableName, getItemRequest);
            if(null == returned_item || returned_item.isEmpty()) 
            {
            	isFirstTimeIngest = true;
            	IngestIO.COLUMN_AUDIT_INGEST_TABLE.forEach((column)->ingestAuditrecordEntityVO.getRecords().put(column, IngestIO.VALUE_HYPHEN));
            }
            else 
            {
            	isFirstTimeIngest = false;
            	lastProcessedIngestS3File = returned_item.get(IngestIO.COLUMN_FILE_NAME).getS();
            	returned_item.forEach((column,attribute) -> ingestAuditrecordEntityVO.getRecords().put(column, attribute.getS()));
            	IngestLogger.info("Last S3 Ingest file processed: "+lastProcessedIngestS3File);
            }
        }catch(Exception e) {
        	IngestLogger.info("Exception while fetching last processed file info from audit table");
        	throw new IngestException("Processing error detected",e);
        }
        
        	
    }

    
    
    private void insertIngestTableWithLatestProcessedFileInfo() throws IngestException {
    	List<WriteRequest> requests = new ArrayList<>();
    	int ingestTableEntryCount = isFirstTimeIngest?1:2;
    	try {
    		for(int i=0;i<ingestTableEntryCount;i++) {
        		if(isFirstTimeIngest || i==1) {
        			setFieldsForIngestTableRecord();
        		}else if(i==0) {
        			ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_UPDATE_DATE, CommonUtil.getCurrentDateTime(new Date()));
        		}
        			
        		HashMap<String,AttributeValue> itemValues = new HashMap<String,AttributeValue>();
    			if(null!=ingestAuditrecordEntityVO.getRecords()) {
            		ingestAuditrecordEntityVO.getRecords().forEach((column,attributeValue)->itemValues.put(column, new AttributeValue().withS(attributeValue)));
            			
            	}
            	PutRequest putRequest = new PutRequest(itemValues);
            	WriteRequest writeRequest = new WriteRequest(putRequest);
            	requests.add(writeRequest);
        	}
        	
        	
        	dynamoDB.batchWrite(ingestTableName, requests);
    	}catch (Exception e) {
    		IngestLogger.info("Error happened while processign ingest audit record");
    		throw new IngestException("Error happened while processign ingest audit record::"+e.getMessage());
		}
    	
	}

    
    /**
     * 
     * @param accountNumber
     * @param line
     * @throws IOException
     */
    private void compareRecordByAccountIdForDeltaChange(String accountNumber,String line) throws IOException {

    	char [] buffer = new char[1];
    	boolean newRecord = true;
    	StringBuilder processedFileBuilder = new StringBuilder();
    	
    	/**
    	 * If the last record check results in an insert operation then that is new record
    	 * As the record are sorted by AccountID the new record should fit in three places, considering that records are sorted by accountID
    	 * 1. At the start (The first row)
    	 * 2. In between two existing records such as new 105(accountID) will sit in between old 104(accountID) and 106(accountID)
    	 * 3. At the end (The last row)
    	 * For any record detected at the immediate previous step, the next compare should start with the same line as compared with previous record
    	 *** E.g. New 104(accountID) is being compared with old 104(accountID). And no change/update detected
    	 *** Now new 105(accountID) record is being compared with old 106(accountID)[which comes after 104] record
    	 *** Then 105(accountID) has to be inserted in DB between 104(accountID) and 106(accountID)
    	 *** In the next compare new 106(accountID)should be compared with same 106(accountID) from old file instead of next to 106(accountID)
    	 *** In this case new line will not be read from old file, rather last processed line will picked up again for comparison
    	 * lastLineRead=> This will hold the last line read. 
    	 */
    	if(lastCompareStatus.equals(IngestIO.DB_OPERATION_INSERT)) {
    		String processedFileLine = lastLineRead.toString();
    		String [] values = processedFileLine.split(",");
    		StringBuffer sb = new StringBuffer();
    		for(int i = 0; i < values.length; i++) 
    		{
    	         sb.append(values[i]);
    	    }
    	    String str = sb.toString();
    	    String processedAccountNumber = values[IngestIO.INDEX_ACCOUNTID];
			newRecord = compareRecord(accountNumber, line, processedFileLine, processedAccountNumber);
			
    	}
    	
    	/**
    	 * When accountId match found between old and new file during previous comparison
    	 * Start the next comparison from the next line in old file
    	 */
    	else {
    		while (processedFileReader.read(buffer) != -1) {
        		if(buffer[0] == '\n') {
        			if(isHeader) {
        				IngestLogger.info("Skipping the header for processed file");
        				isHeader = false;
        				continue;
        			}
        			String processedFileLine = processedFileBuilder.toString().trim();
        			String [] values = processedFileLine.split(",");
        			String processedAccountNumber = values[IngestIO.INDEX_ACCOUNTID];
        			newRecord = compareRecord(accountNumber, line, processedFileLine, processedAccountNumber);
        			processedFileBuilder.setLength(0);
        			break;
        		}
        		if(!isHeader)
        			processedFileBuilder.append(buffer);
        	}
    		
    		// Processing the last record in old file
    		if(processedFileBuilder.length() > 0) {
        		String processedFileLine = processedFileBuilder.toString().trim();
    			String [] values = line.split(",");
    			String processedAccountNumber = values[IngestIO.INDEX_ACCOUNTID];
    			newRecord = compareRecord(accountNumber, line, processedFileLine, processedAccountNumber);
    			processedFileBuilder.setLength(0); 
    		  }
    	}
    	
    		
    	if(newRecord) {
    		lastCompareStatus = IngestIO.DB_OPERATION_INSERT;
    		
    	}
    
    
    }
    
    /**
     * This will compare the records from current and last processed file
     * The primary key is AccountId to matching
     * @param accountNumber
     * @param line
     * @param processedFileLine
     * @param processedAccountNumber
     * @return
     */
    private boolean compareRecord(String accountNumber, String line, String processedFileLine,
			String processedAccountNumber) {
		boolean newRecord=false;
		if(StringUtils.isNotEmpty(processedAccountNumber))
		{
			if(Integer.parseInt(processedAccountNumber) == Integer.parseInt(accountNumber)) {
				
				if(line.hashCode() == processedFileLine.hashCode()) {
					lastCompareStatus = IngestIO.DB_OPERATION_NOACTION;
				}else {
					lastCompareStatus = IngestIO.DB_OPERATION_UPDATE;
				}
				
			}else{
				newRecord = true;
				lastCompareStatus = IngestIO.DB_OPERATION_INSERT;
				lastLineRead.setLength(0);
				lastLineRead.append(processedFileLine);
			}
			
		}else {
			newRecord = false;
			
		}
			 
		return newRecord;
	}
    
    /**
     * This method will populate table attributes
     * Table: <env>-ftacel-ingest-ddb
     * Columns: IngestId/Reocrds count/Create-Update date
     */
    private void setFieldsForIngestTableRecord() {
    	
    	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_RECORD_COUNT, String.valueOf(recordCount));
    	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_INSERT_COUNT, String.valueOf(recordInsertCount));
    	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_CREATE_DATE, CommonUtil.getCurrentDateTime(new Date()));
    	ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_UPDATE_DATE, ingestAuditrecordEntityVO.getRecords().get(IngestIO.COLUMN_CREATE_DATE));
    	
    	if(!isFirstTimeIngest) {
			ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_FILE_NAME, currentProcessingIngestS3File);
			ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_INGEST_ID, IngestIO.STATUS_CURRENT);
			ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_UPDATE_COUNT, String.valueOf(recordUpdateCount));
			ingestAuditrecordEntityVO.getRecords().put(IngestIO.COLUMN_NO_CHANGE, String.valueOf(recordNoChangeCount));
		}
		
		
	}
    
    /**
     * Clear all cache for ingest
     */
    private void cleanupIngestCache()
    {
        IngestLogger.info("Cleaning up failed ingest after error");
        inbound.clear();
        pleaseStop();
        waitForAll();
        workers.clear();
        threads.clear();
        IngestLogger.info("Finished cleaning up after error");
    }
    
    /**
     * Cache clear
     * This will reset all variables for file compare to default value for next time use
     */
    private void resetCompareVariable() {
    	isFirstTimeIngest = false;
    	lastProcessedIngestS3File = "";
        currentProcessingIngestS3File = "";
        lastCompareStatus = "";
        ingestAuditrecordEntityVO =  new RecordEntityVO();
        recordCount=0;
        recordInsertCount = 0;
        recordUpdateCount = 0;
        recordNoChangeCount = 0;
        lastLineRead = new StringBuilder();
        processedFileReader=null;
        isHeader = true;
    }
    
    private BufferedReader decryptS3File(S3ObjectInputStream in) throws NoSuchProviderException, IOException {
    	InputStream plaintextStream = BouncyGPG
                .decryptAndVerifyStream()
                .withConfig(keyringConfig)
                .andIgnoreSignatures()
                .fromEncryptedInputStream(in);
             BufferedReader s3Reader = new BufferedReader(new InputStreamReader(plaintextStream), 65536);
             return s3Reader;
    }
    
    private BufferedReader loadBufferReaderFromS3Bucket(String bucket, String fileName) throws IngestException{
	    	
	    	try {
	    		S3Object s3Object = s3Manager.getObject(new S3Path(bucket, fileName));
	        	InputStream plaintextStream = BouncyGPG
	                    .decryptAndVerifyStream()
	                    .withConfig(keyringConfig)
	                    .andIgnoreSignatures()
	                    .fromEncryptedInputStream(s3Object.getObjectContent());
	                 BufferedReader processedFileReader = new BufferedReader(new InputStreamReader(plaintextStream), 65536);
	                 return processedFileReader;
	    	}catch (Exception e) {
				IngestLogger.info("Could not read from S3 bucket:"+bucket+" for the file:"+fileName);
				throw new IngestException("Could not read from S3 bucket:"+bucket+" for the file:"+fileName);
			}
	    	
	    }
   
}

