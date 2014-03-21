package com.jobs.utils;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.utils]
 * @ClassName:    [Config]
 * @Description:  [配置文件加载类]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:36:05]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:36:05]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class Config {
	
	private static final Logger LOG = LoggerFactory.getLogger(Config.class);
	
	public static String HDFS_ROOT;

	/*public static String getHdfsRoot() {
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(Constants.LOCAL_CONFIG_HADOOP));
			Properties p = new Properties();
			p.load(in);
			return p.getProperty("hdfs.root");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}*/
	
	static {
		Properties properties = new Properties();
		try {
			properties.load(ClassLoaderUtil.getResourceAsStream("config/hadoop.properties", Config.class));
			HDFS_ROOT = properties.getProperty("hdfs.root");
		} catch (Throwable e) {
			e.printStackTrace();
			LOG.error("=======加载hadoop配置文件失败========");
		}
	}

	/**
	 * main 函数
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(HDFS_ROOT);

	}

}
