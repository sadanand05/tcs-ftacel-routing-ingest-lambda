package com.foxtel.ingest.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * JSON utils
 * Author: Josh Passenger <jospas@amazon.com>
 */
public class JsonUtils
{
    private static Gson gson = new GsonBuilder().create();
    private static Gson gsonPretty = new GsonBuilder().setPrettyPrinting().create();

    public static String toJson(Object o)
    {
        return gson.toJson(o);
    }

    public static String toJsonPretty(Object o)
    {
        return gsonPretty.toJson(o);
    }

    public static Object fromJson(String json, Class clazz)
    {
        return gson.fromJson(json, clazz);
    }
}
