package com.foxtel.ingest.aws.dynamodb;

import com.foxtel.ingest.exception.IngestException;

/**
 * Action interface that allows for retrying actions
 * @param <T> the templated return type from execute()
 */
public interface DynamoDBAction<T>
{
    void setData(Object data);
    T execute() throws IngestException;
    String getName();
    boolean isComplete();
}
