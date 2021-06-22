package com.foxtel.ingest.exception;

/**
 * Ingest exception
 * Author: Josh Passenger <jospas@amazon.com>
 */
public class IngestException extends Throwable
{
    public IngestException(String message)
    {
        super(message);
    }

    public IngestException(String message, Throwable t)
    {
        super(message, t);
    }
}
