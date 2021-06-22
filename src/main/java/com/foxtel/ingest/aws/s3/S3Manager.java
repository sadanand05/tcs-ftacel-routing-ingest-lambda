package com.foxtel.ingest.aws.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.foxtel.ingest.exception.IngestException;
import com.foxtel.ingest.logger.IngestLogger;
import org.apache.commons.io.IOUtils;

/**
 * Helper functions for S3
 */
public class S3Manager
{
    private final AmazonS3 s3;

    public S3Manager(String region)
    {
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxConnections(10)
                .withTcpKeepAlive(true)
                .withThrottledRetries(false)
                .withMaxErrorRetry(10)
                .withRequestTimeout(120000)
                .withClientExecutionTimeout(0);

        s3 = AmazonS3ClientBuilder.standard()
                .withClientConfiguration(clientConfig)
                .withRegion(region).build();
    }

    public String readObjectAsString(S3Path path) throws IngestException
    {
        try
        {
            S3Object s3Object = s3.getObject(path.getBucket(), path.getKey());
            S3ObjectInputStream in = s3Object.getObjectContent();
            String result = IOUtils.toString(in, "UTF-8");
            s3Object.close();
            return result;
        }
        catch (Throwable t)
        {
            IngestLogger.error("Failed to load S3 object as string: " + path, t);
            throw new IngestException("Failed to load S3 object as string: " + path, t);
        }
    }

    public S3Object getObject(S3Path path) throws IngestException
    {
        try
        {
            return s3.getObject(path.getBucket(), path.getKey());
        }
        catch (Throwable t)
        {
            IngestLogger.error("Failed to load S3 object: " + path, t);
            throw new IngestException("Failed to load S3 object: " + path, t);
        }
    }
}
