package com.jobs.utils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.utils]
 * @ClassName:    [TimestampUtils]
 * @Description:  [日期格式化工具集]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午3:14:05]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午3:14:05]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class TimestampUtils {

	public static String convert(Timestamp microTimestamp) {
		String timeStampString = Long.toString(microTimestamp.getTime() / ScheduleConstants.THOUSAND * ScheduleConstants.MILLION
				+ microTimestamp.getNanos() / ScheduleConstants.THOUSAND);
		return timeStampString;
	}

	public static Timestamp convert(long microTimestamp) {
		Timestamp nanoTimestamp = new Timestamp(microTimestamp / ScheduleConstants.THOUSAND);
		nanoTimestamp.setNanos((int) (microTimestamp % ScheduleConstants.MILLION * ScheduleConstants.THOUSAND));
		return nanoTimestamp;
	}

	public static String formatDuring(long mss) {
		DecimalFormat df = new DecimalFormat(ScheduleConstants.STR_FORMAT);
		long hours = (mss % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
		long minutes = (mss % (1000 * 60 * 60)) / (1000 * 60);
		long seconds = Math.round((float)(mss % (1000 * 60)) / 1000);
		return df.format(hours) + ":" + df.format(minutes) + ":" + df.format(seconds);
	}
	
	public static Date String2Date(String arg) throws ParseException{
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
		Date date = format.parse(arg); 
		return date;
	}
	
	public static String Date2String(Date arg){
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
		
		return format.format(arg);
	}

	public static void main(String[] args) throws Exception{
		System.out.println(String2Date("2013-11-29 11:04:34").toString());
		
		System.out.println(Date2String(new Date()));
		
		System.out.println(formatDuring(38707));
	}

}
