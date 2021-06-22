package com.foxtel.ingest.aws.s3;

import com.foxtel.ingest.exception.IngestException;

/**
 * Helper class for S3 paths
 */
public class S3Path
{
    private final String bucket;
    private final String key;

    public S3Path(String s3Path) throws IngestException
    {
        if (s3Path == null)
        {
            throw new IngestException("Null S3Path");
        }

        if (!s3Path.startsWith("s3://"))
        {
            throw new IngestException("Invalid S3 path: " + s3Path);
        }

        int nextSlash = s3Path.indexOf('/', 8);

        if (nextSlash == -1)
        {
            bucket = s3Path.substring(5);
            key = "";
        }
        else
        {
            bucket =  s3Path.substring(5, nextSlash);
            key = s3Path.substring(nextSlash + 1);
        }
    }

    public S3Path(String bucket, String key)
    {
        this.bucket = bucket;
        this.key = key;
    }

    public String getBucket()
    {
        return bucket;
    }

    public String getKey()
    {
        return key;
    }

    public String toString()
    {
        return String.format("s3://%s/%s", bucket, key);
    }
}
