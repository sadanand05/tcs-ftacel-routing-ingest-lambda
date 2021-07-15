package com.foxtel.ingest.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonUtil {

	public static String getCurrentDateTime(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		String dateValue = format.format(date).toString();
		return dateValue;
	}
}
