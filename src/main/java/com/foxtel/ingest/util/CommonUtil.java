package com.foxtel.ingest.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

public class CommonUtil {

	public static String getCurrentDateTime (Date date) 
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		format.setTimeZone(TimeZone.getTimeZone(Calendar.getInstance().getTimeZone().getID()));
		String dateValue = format.format(date).toString();
		return dateValue;
	}
	
	public static String getUUID () 
	{
		return String.valueOf(UUID.randomUUID());
	}
}
