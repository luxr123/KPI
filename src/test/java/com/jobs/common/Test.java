package com.jobs.common;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.hbase.util.Bytes;

public class Test {

	public static void main(String[] args) throws UnsupportedEncodingException {
		String s = "http%3A%2F%2Ffans.51job.com%2Fpayservice%2Ffans%2Ffans_index.php";
//		String s = "http://fans.51job.com/payservice/fans/fans_index.php";
		System.out.println(URLDecoder.decode(s,"UTF-8"));
		
		System.out.println(Bytes.toString("\\xE5\\xB9\\xBF\\xE4\\xB8\\x9C".getBytes("utf-16")));
		
		System.out.println(new File("e:/qqwry.dat").getName().toLowerCase());
	}
}
