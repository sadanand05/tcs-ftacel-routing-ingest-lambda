package com.foxtel.ingest.aws.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.*;
import com.foxtel.ingest.exception.IngestException;
import com.foxtel.ingest.logger.IngestLogger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Helper class to robustly insert large numbers of records into DynamoDB
 */
public class DynamoDBManager
{
    private final AmazonDynamoDB dynamoDB;

    private static Random random = new Random();

    public DynamoDBManager(String region, int maxConnections)
    {
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxConnections(maxConnections)
                .withTcpKeepAlive(true)
                .withThrottledRetries(false)
                .withMaxErrorRetry(0)
                .withRequestTimeout(5000)
                .withClientExecutionTimeout(0);

        dynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withClientConfiguration(clientConfig)
            .withRegion(region).build();
    }

    /**
     * Perform some batch insert, update or delete operation
     * @param tableName the table name
     * @param requests  the requests
     * @throws IngestException thrown on failure
     */
    public void batchWrite(final String tableName, List<WriteRequest> requests) throws IngestException
    {
        Map<String, List<WriteRequest>> requestMap = new HashMap<String, List<WriteRequest>>();
        requestMap.put(tableName, requests);

        DynamoDBAction<Void> batchWrite = new DynamoDBAction<Void>()
        {
            private Map<String, List<WriteRequest>> requestMap = null;

            public void setData(Object data)
            {
                this.requestMap = (Map<String, List<WriteRequest>>) data;
            }

            public boolean isComplete()
            {
                return requestMap.isEmpty() || requestMap.get(tableName).isEmpty();
            }

            public Void execute() throws IngestException
            {
                BatchWriteItemResult result = dynamoDB.batchWriteItem(this.requestMap);
                requestMap = result.getUnprocessedItems();
                return null;
            }

            public String getName()
            {
                return "Batch writing to DynamoDB table: " + tableName;
            }
        };

        batchWrite.setData(requestMap);

        // Keep writing while we are not complete
        while (!batchWrite.isComplete())
        {
            executeAction(batchWrite);
        }
    }

    /**
     * Fetches an item from DynamoDB
     * @param tableName the table name
     * @param request  the request
     * @return the loaded item
     * @throws IngestException thrown on failure
     */
    public Map<String, AttributeValue> getItem(final String tableName, GetItemRequest request) throws IngestException
    {
        DynamoDBAction<Map<String, AttributeValue>> getItem = new DynamoDBAction<Map<String, AttributeValue>>()
        {
            private GetItemRequest request = null;

            public void setData(Object data)
            {
                this.request = (GetItemRequest) data;
            }

            public boolean isComplete()
            {
                return true;
            }

            public Map<String, AttributeValue> execute() throws IngestException
            {
                GetItemResult result = dynamoDB.getItem(this.request);
                return result.getItem();
            }

            public String getName()
            {
                return "Fetching item from DynamoDB table: " + tableName;
            }
        };

        getItem.setData(request);

        // Keep writing while we are not complete
        return executeAction(getItem);
    }

    /**
     * Run an DynamoDB action retrying with exponential backup
     *
     * @param action the action to run
     * @param <T>    the templated return type
     * @return the templated return type for this action
     * @throws IngestException on failure
     */
    @SuppressWarnings("UnusedReturnValue")
    public <T> T executeAction(DynamoDBAction<T> action) throws IngestException
    {
        int maxRetries = 10;
        int retryCount = 0;

        Throwable lastError = null;

        while (retryCount < maxRetries)
        {
            try
            {
                return action.execute();
            }
            catch (ResourceNotFoundException e)
            {
                throw e;
            }
            catch (ConditionalCheckFailedException e)
            {
                throw new IngestException("DynamoDB conditional check failure", e);
            }
            catch (Throwable t)
            {
                lastError = t;
            }

            long sleepTime = getSleepTime(retryCount);

            if (retryCount > 5)
            {
                String message = String.format("DynamoDBOperation [%s] failed in retry [%d of %d] sleeping for [%d] cause: %s",
                        action.getName(), retryCount, maxRetries, sleepTime, lastError.toString());
                IngestLogger.warn(message);
            }
            sleepFor(sleepTime);
            retryCount++;
        }

        throw new IngestException("DynamoDB persistent failure, giving up", lastError);
    }

    /**
     * Fetches a random jittered amount of time to sleep for the given retry
     * @param retry the retry index
     * @return a randomly jittered sleep time
     */
    public static long getSleepTime(int retry)
    {
        long maxSleep = Math.min(6400L, (long) (Math.pow(2, retry) * 50L));
        return (long) (random.nextFloat() * maxSleep);
    }

    /**
     * Sleeps for the requested time period
     *
     * @param millis the time to sleep in millis
     */
    public void sleepFor(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            // Ignored
        }
    }
}
