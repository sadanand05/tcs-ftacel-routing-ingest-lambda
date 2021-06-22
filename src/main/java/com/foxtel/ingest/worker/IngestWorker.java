/*
 Author Josh Passenger <jospas@amazon.com>
 */

package com.foxtel.ingest.worker;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.foxtel.ingest.aws.dynamodb.DynamoDBManager;
import com.foxtel.ingest.exception.IngestException;
import com.foxtel.ingest.lambda.IngestLambda;
import com.foxtel.ingest.logger.IngestLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Handles making batch inserts into DynamoDB
 */
public class IngestWorker implements Runnable
{
    private final IngestLambda ingestLambda;
    private final String tableName;
    private final DynamoDBManager dynamoDB;
    private final LinkedBlockingQueue<WriteRequest> inbound;

    private final List<WriteRequest> requests = new ArrayList<>();

    private volatile boolean stopping = false;
    private boolean errored = false;
    private Throwable cause = null;

    public IngestWorker(IngestLambda ingestLambda,
                        String tableName,
                        DynamoDBManager dynamoDB,
                        LinkedBlockingQueue<WriteRequest> inbound)
    {
        this.ingestLambda = ingestLambda;
        this.tableName = tableName;
        this.dynamoDB = dynamoDB;
        this.inbound = inbound;
    }

    public void pleaseStop()
    {
        this.stopping = true;
    }

    @Override
    public void run()
    {
        try
        {
            while (!stopping)
            {
                // Read a write request
                WriteRequest writeRequest = inbound.poll(1000L, TimeUnit.MILLISECONDS);

                // Process a write request
                if (writeRequest != null)
                {
                    requests.add(writeRequest);

                    if (requests.size() == 25)
                    {
                        processRequests();
                        requests.clear();
                    }
                }
            }

            processRequests();
        }
        catch (InterruptedException e)
        {
            // Ignore
        }
        catch (Throwable t)
        {
            IngestLogger.error("Critical error detected, stopping worker thread", t);
            errored = true;
            cause = t;
            ingestLambda.markErrored();
        }
    }

    private void processRequests() throws IngestException
    {
        if (requests.isEmpty())
        {
            return;
        }

        dynamoDB.batchWrite(tableName, requests);
    }

    public boolean isErrored()
    {
        return errored;
    }

    public Throwable getCause()
    {
        return cause;
    }
}
