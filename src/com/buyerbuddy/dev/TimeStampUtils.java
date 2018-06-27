package com.buyerbuddy.dev;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Methods for dealing with timestamps
 */
public class TimeStampUtils {

	/**
	 * Return an ISO 8601 combined date and time string for current date/time
	 * 
	 * @return String with format "yyyy-MM-dd'T'HH:mm:ss'Z'"
	 */
	public static String getISO8601StringForCurrentDate() {
		Date now = new Date();
		return getISO8601StringForDate(now);
	}
	
	// Get yesterday
	public static String getISO8601StringForYesterday() {
		Date now = new Date();
		Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.DATE, -1);
        Date yesterday = cal.getTime();
		return getISO8601StringForDate(yesterday);
	}
	

	/**
	 * Return an ISO 8601 combined date and time string for specified date/time
	 * 
	 * @param date
	 *            Date
	 * @return String with format "yyyy-MM-dd'T'HH:mm:ss'Z'"
	 */
	private static String getISO8601StringForDate(Date date) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return dateFormat.format(date);
	}

	/**
	 * Private constructor: class cannot be instantiated
	 */
	private TimeStampUtils() {
	}
}