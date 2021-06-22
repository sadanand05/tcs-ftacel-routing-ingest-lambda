package com.foxtel.ingest.logger;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Logger wrapping the Lambda logger
 * Author: Josh Passenger <jospas@amazon.com>
 */
public class IngestLogger
{
    private static LambdaLogger LOGGER = null;

    public static void setLogger(LambdaLogger logger)
    {
        LOGGER = logger;
    }

    public static void info(String message)
    {
        if (LOGGER != null)
        {
            LOGGER.log("[INFO] " + message);
        }
        else
        {
            System.out.println("[INFO] " + message);
        }
    }

    public static void warn(String message)
    {
        if (LOGGER != null)
        {
            LOGGER.log("[WARN] " + message);
        }
        else
        {
            System.out.println("[WARN] " + message);
        }
    }

    public static void error(String message, Throwable t)
    {
        StringWriter out = new StringWriter();
        PrintWriter  writer = new PrintWriter(out);
        t.printStackTrace(writer);

        if (LOGGER != null)
        {
            LOGGER.log("[ERROR] " + message + "\nCause: " + out.toString());
        }
        else
        {
            System.out.println("[ERROR] " + message + "\nCause: " + out.toString());
        }
    }

}
