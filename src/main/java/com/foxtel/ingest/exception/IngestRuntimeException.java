package com.foxtel.ingest.exception;

/**
 * Ingest exception
 * Author: Josh Passenger <jospas@amazon.com>
 */
public class IngestRuntimeException extends RuntimeException
{
    public IngestRuntimeException(String message)
    {
        super(message);
    }

    public IngestRuntimeException(String message, Throwable t)
    {
        super(message, t);
    }
}