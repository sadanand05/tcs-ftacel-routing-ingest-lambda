package com.foxtel.ingest.lambda;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.foxtel.ingest.aws.dynamodb.DynamoDBManager;
import com.foxtel.ingest.aws.s3.S3Manager;
import com.foxtel.ingest.aws.s3.S3Path;
import com.foxtel.ingest.aws.secrets.SecretsHelper;
import com.foxtel.ingest.exception.IngestException;
import com.foxtel.ingest.exception.IngestRuntimeException;
import com.foxtel.ingest.json.JsonUtils;
import com.foxtel.ingest.logger.IngestLogger;
import com.foxtel.ingest.worker.IngestWorker;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.callbacks.KeyringConfigCallbacks;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Lambda function that ingests data files and inserts them into DynamoDB
 * Author: Josh Passenger <jospas@amazon.com>
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

    private static final String VERSION = "20210616-001 initial version";

    public IngestLambda()
    {
        IngestLogger.info("Cold start for version: " + VERSION);
        BouncyGPG.registerProvider();
        this.region = System.getenv("REGION");
        this.customerTableName = System.getenv("CUSTOMER_TABLE_NAME");
        this.ingestTableName = System.getenv("INGEST_TABLE_NAME");
        this.threadCount = Integer.parseInt(System.getenv("THREAD_COUNT"));
        this.dynamoDB = new DynamoDBManager(region, threadCount);
        this.s3Manager = new S3Manager(region);
        this.secretsHelper = new SecretsHelper(region);
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

        for (SQSMessage msg : event.getRecords())
        {
            IngestLogger.info("Received request message: " + msg.getBody());

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

        IngestLogger.info("Ingest is complete");
        return null;
    }

    /**
     * Resets the system for the next ingest
     */
    private void init() throws IngestException
    {
        // Reload the private key and secret each run
        String privateKey = secretsHelper.getSecretString(System.getenv("SECRETS_MANAGER_KEY_ARN"));
        String passphrase = secretsHelper.getSecretString(System.getenv("SECRETS_MANAGER_PASSPHRASE_ARN"));
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
            return;
        }

        long start = System.currentTimeMillis();

        init();

        IngestLogger.info("Ingesting input encrypted CSV: " + inputPath);

        try (S3Object s3Object = s3Manager.getObject(inputPath))
        {
            IngestLogger.info("Found object length: " + s3Object.getObjectMetadata().getContentLength() + " bytes");
            processStream(inputPath, s3Object.getObjectContent());
        }
        catch (IngestException e)
        {
            throw e;
        }
        catch (Throwable t)
        {
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
        try (InputStream plaintextStream = BouncyGPG
                .decryptAndVerifyStream()
                .withConfig(keyringConfig)
                .andIgnoreSignatures()
                .fromEncryptedInputStream(in);
             BufferedReader s3Reader = new BufferedReader(new InputStreamReader(plaintextStream), 65536))
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
                        if (processLine(line, columns))
                        {
                            itemCount++;
                        }
                    }

                    builder.setLength(0);
                }

                builder.append(buffer);
            }

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
     * Processes a line into a Dynamo write request
     * @param line the line to process
     * @param columns the columns detected
     * @return true if the line was process, false if it was blank
     * @throws IngestException thrown on failure
     */
    private boolean processLine(String line, String [] columns) throws IngestException
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

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("AccountId", new AttributeValue().withS(values[3]));

        if (StringUtils.isNotBlank(values[1]))
        {
            item.put("PhoneNumber1", new AttributeValue().withS(values[1]));
        }

        if (StringUtils.isNotBlank(values[2]))
        {
            item.put("PhoneNumber2", new AttributeValue().withS(values[2]));
        }

        Map<String, String> attributes = new TreeMap<>();

        for (int i = 0; i < columns.length; i++)
        {
            if (StringUtils.isNotBlank(values[i]))
            {
                attributes.put(columns[i], values[i]);
            }
        }

        item.put("Attributes", new AttributeValue().withS(JsonUtils.toJson(attributes)));

        PutRequest putRequest = new PutRequest(item);
        WriteRequest writeRequest = new WriteRequest(putRequest);
        putInbound(writeRequest);

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
        IngestLogger.info("Cleaning up failed ingest after error");
        inbound.clear();
        pleaseStop();
        waitForAll();
        workers.clear();
        threads.clear();
        IngestLogger.info("Finished cleaning up after error");
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
}

