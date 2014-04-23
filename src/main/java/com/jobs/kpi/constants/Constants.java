package com.jobs.kpi.constants;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.kpi.constants]
 * @ClassName:    [Constants]
 * @Description:  [定义全局常量]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:11:58]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:11:58]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class Constants {

	public static final String UTF8 = "UTF-8";
	public static final String COMMA = ",";
	public static final String BACKSLASH = "/";         			 // /----backslash 反斜线
	public static final String SLASH = "\\";           				 // \----slash 正斜线
	public static final String AND = "&";
	public static final String LOG_DELIMITER = "";     			 // ^Z符号 .  \\x1A
	public static final String MARK = "`";     				         // ` 符号
	public static final String UNDERLINE = "_";
	public static final String HYPHEN = "-";
	public static final String EQUAL = "=";

	public static final int KPILENGTH = 11;
	public static final String TABLENAME_DOMAIN = "kpi_domain";
	public static final String TABLENAME_PAGE = "kpi_page";
	public static final String TABLENAME_DETAIL = "kpi_detail";
	public static final String TABLENAME_CONFIG_DOMAIN = "config_domain";
	public static final String CF = "cf";
	public static final String PV = "pv";
	public static final String UV = "uv";
	public static final String COUNT = "count";
	public static final String FILTER = "filter";
	public static final String PATTERN = "pattern";
	public static final String ROWKEY_MY = "my";
	
	public static byte[] CF_BYTES = Bytes.toBytes(CF);
	public static byte[] CF_COUNT = Bytes.toBytes(COUNT);
	
	
	public static final String REMOTE_IP = "remote_ip";							// 记录客户端的ip地址
	public static final String WID = "wid";										// 记录客户端cookie中的id地址
	public static final String VISIT_PAGE = "visit_page";						// 记录访问页面
	public static final String QUERY_STRING = "query_string";					// 记录查询字符串
	public static final String STATUS = "status";								// 记录请求状态；成功是200
	public static final String HTTP_REFERER = "http_referer";					// 用来记录从那个页面链接访问过来的
	public static final String DOMAIN = "domain";								// 记录域名
	public static final String PAGE = "page";									// 记录页面
	public static final String REFERER = "referer";								// 记录referer
	public static final String JOB_PAGE = "job_page";							// 记录埋点源
	public static final String PAGE_AVG_TIME = "m_page_avg_time";				// 记录页面停留时间
	public static final String PAGE_VISIT_TIME = "page_visit_time";				// 记录页面访问时间
	public static final String RESPONDENTS_DOMAIN = "respondents_domain";		// 记录受访域名
	public static final String ACCOUNT_PAGE = "account_page";					// 记录想要统计的页面
	public static final String CLICK_MARK = "click_mark";					    // 记录被统计页面的点击标识
	public static final String IS_ENTRY = "is_entry";					    	// 记录是否为入口页面
	public static final String IS_NEW = "is_new";					    		// 记录新访客
	public static final String ADSREF = "adsref";					    		// 记录广告来源
	public static final String ADSNEW = "adsnew";					    		// 为1表示广告入口, 为0表示旧广告
	public static final String ADSID = "adsid";					    		    // 记录广告ID
	public static final String HOT = "hot";					    		    	// 记录热击链接
	public static final String WB = "wb";					    		    	// 记录外部链接
	public static final String SE = "se";					    		    	// 记录为搜索引擎
	public static final String KW = "kw";					    		    	// 记录为搜索词
	
	
	
	public static final String HDFS_IP_FILE = 
			"hdfs://10.100.2.2:54310/qqwry.dat";							// 纯真IP数据库名
	public static String IP_FILE = "qqwry.dat";   								// 纯真IP数据库名
	public static final String INSTALL_DIR = 
			"/home/hadoop/hadoop/tmp/nm-local-dir/usercache/hadoop/appcache";   // 纯真IP数据库的存放路径
//	public static final String INSTALL_DIR = "e:";					    		// 纯真IP数据库的存放路径
	
}
