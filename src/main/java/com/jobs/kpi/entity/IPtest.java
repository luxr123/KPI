package com.jobs.kpi.entity;

import com.jobs.kpi.ip.service.IPSeeker;

/**
 * User: xiaorui.lu Date: 2014年1月20日 下午5:33:57
 */
public class IPtest {
	

	public static void main(String[] args) {
		// 指定纯真数据库的文件名，所在文件夹
		IPSeeker ipSeeker = IPSeeker.getInstance();
		String ip = "221.120.191.255";
		// 测试IP 58.20.43.13
		System.out.println(ipSeeker.getIPLocation(ip).getProvince());
		System.out.println(ipSeeker.getIPLocation(ip).getCity());
		System.out.println(ipSeeker.getIPLocation(ip).getArea());
		
		
		
	}
}
