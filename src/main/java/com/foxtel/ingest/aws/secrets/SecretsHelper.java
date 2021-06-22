package com.foxtel.ingest.aws.secrets;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.foxtel.ingest.exception.IngestException;
import com.foxtel.ingest.logger.IngestLogger;

/**
 * Helper to load secrets from AWS Secrets Manager
 */
public class SecretsHelper
{
    private AWSSecretsManager secrets;

    public SecretsHelper(String region)
    {
        secrets = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region).build();
    }

    public String getSecretString(String arn) throws IngestException
    {
        try
        {
            GetSecretValueRequest request = new GetSecretValueRequest().withSecretId(arn);
            return secrets.getSecretValue(request).getSecretString();
        }
        catch (Throwable t)
        {
            IngestLogger.error("Failed to load secret: " + arn, t);
            throw new IngestException("Failed to fetch secret: " + arn, t);
        }
    }
}
