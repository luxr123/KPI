package com.jobs.kpi.model;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.jobs.kpi.constants.Constants;

public class KPITest {

	/**
	 * 
	 * @param 要转换的毫秒数
	 * @return 该毫秒数转换为 * days * hours * minutes * seconds 后的格式
	 */
	public String formatDuring(long mss) {
		long days = mss / (1000 * 60 * 60 * 24);
		long hours = (mss % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
		long minutes = (mss % (1000 * 60 * 60)) / (1000 * 60);
		long seconds = (mss % (1000 * 60)) / 1000;
		return hours + ":" + minutes + ":" + seconds;
	}
	
	private static final String STR_FORMAT = "00";   
	  
	public static String testDecimal(long liuShuiHao){  
	    DecimalFormat df = new DecimalFormat(STR_FORMAT);
	    return df.format(liuShuiHao);  
	}  

	@Test
	public void test() {
		System.out.println(formatDuring(641041));
		System.out.println(testDecimal(2));
	}
	
	@Test
	public void testPattern(){
		Pattern p = Pattern.compile("(?:(?<=a)u.*(?=&)|(?<=a)har(?=&)|(?<=a)onduc|our.*?(?=&))");
		Matcher m = p.matcher("ahar&ss&our&qq&ss");
		if(m.find())
			System.out.println(m.replaceAll("xxxxx"));
	}
	
	@Test
	public void testSubPattern() throws UnsupportedEncodingException{
		String s = "112.241.133.19802013-12-24 18:52:36GET/5a.phpre=http://my.51job.com/test/10000/My_Pmc.php?id=1&id=domain"
				+ "2000http://my.51job.com/my/10000/My_Pmc.php?id=2";
		
		Pattern p = Pattern.compile("(?:(?<=re=)http://my.51job.com/test/[0-9]+/My_Pmc.php?.*(?=&)|(?<=)http://my.51job.com/my/[0-9]+/My_Pmc.php?.*(?=))");
		Matcher m = p.matcher(s);
		if (m.find()) {
			System.out.println(m.replaceAll("http://my.51job.com/my/My_Pmc.php"));
		}
		
		System.out.println(URLDecoder.decode("10.100.50.100^Z0^Z2014-02-25 16:00:00^ZGET^Z/5g.php^Z?wb=1&cd=24&ck=1&la=zh-CN&ws=1440x900&re=http%3A%2F%2Ftrace.51job.com%2Ftrace.php?id=10000000&100&id=xy&st=1393315196986&lnt=3261&cn=0&hot=[{x:272.5,y:615,t:b}]^Z200^Z0^Zhttp%3A%2F%2Fmy.51job.com%2Fmy%2F100000%2FMy_Pmc.php?id=10000000^ZMozilla/5.0 (Windows NT 6.1; rv:26.0) Gecko/20100101 Firefox/26.0^Zwid=5ef03619a7eb3d2dba8e615bbfd6b0277a9aa126; enter=0","UTF-8"));
		
		System.out.println("http://my.51job.com/my/[0-9]+/My_Pmc.php?.*http://my.51job.com/my/My_Pmc.php".split(Constants.LOG_DELIMITER)[1]);
		
		System.out.println(URLEncoder.encode("&&xxx", "UTF-8"));
	}
}
