package com.jobs.kpi.hbase.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;

import com.jobs.kpi.constants.Constants;
import com.jobs.kpi.hbase.dao.HBaseDao;
import com.jobs.utils.ClassLoaderUtil;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.kpi.hbase.service]
 * @ClassName:    [HBaseService]
 * @Description:  [操作hbase的业务层]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:15:20]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:15:20]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
@SuppressWarnings("serial")
public class HBaseService {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseService.class);

	private static HBaseAdmin admin;
	private static HbaseTemplate hbaseTemplate;
	private static Configuration configuration;

	static HBaseDao hBaseDao = null;

	public static Map<String,HashSet<String>> config_domain; //缓存hbase配置
	
	private static Scan scan;
	
	static {
		config_domain = new HashMap<String, HashSet<String>>() {
			{
				put(Constants.FILTER, new HashSet<String>());
				put(Constants.PATTERN, new HashSet<String>());
				put(Constants.DOMAIN, new HashSet<String>());
			}
		};
		try {
			initHBase();
			hbaseTemplate.get(Constants.TABLENAME_CONFIG_DOMAIN, Constants.ROWKEY_MY, new RowMapper<Void>() {
				@Override
				public Void mapRow(Result result, int rowNum) throws Exception {
					for (KeyValue kv : result.raw()) {
						if ("filter".equals(Bytes.toString(kv.getFamily())))
							config_domain.get(Constants.FILTER).add(Bytes.toString(kv.getQualifier()));
						else if ("pattern".equals(Bytes.toString(kv.getFamily())))
							config_domain.get(Constants.PATTERN).add(
									Bytes.toString(kv.getQualifier()) + Constants.LOG_DELIMITER + Bytes.toString(kv.getValue()));
					}
					return null;
				}
			});
			scan = new Scan();
//			Filter filter = new RowFilter(CompareOp.NOT_EQUAL, new RegexStringComparator(Constants.ROWKEY_MY));
//			scan.setFilter(filter);
			hbaseTemplate.find(Constants.TABLENAME_CONFIG_DOMAIN, scan, new RowMapper<Void>() {

				@Override
				public Void mapRow(Result result, int rowNum) throws Exception {
					config_domain.get(Constants.DOMAIN).add(Bytes.toString(result.getRow()));
					return null;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 初始化，加载配置文件
	 * 
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 */
	private static void initHBase() throws MasterNotRunningException, ZooKeeperConnectionException {
		configuration = HBaseConfiguration.create();
		Properties properties = new Properties();
		try {
			properties.load(ClassLoaderUtil.getResourceAsStream("config/hbase.properties", HBaseService.class));

		} catch (Throwable e) {
			e.printStackTrace();
			LOG.error("=======加载hbase配置文件失败========");
		}

		if (configuration == null) {
			configuration = new Configuration();
		}

		configuration.set("hbase.master", properties.getProperty("hbase.master"));
		configuration.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"));
		configuration.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zookeeper.property.clientPort"));
		configuration.set("hbase.regionserver.handler.count", properties.getProperty("hbase.regionserver.handler.count"));
		configuration.set("hbase.client.write.buffer", properties.getProperty("hbase.client.write.buffer"));
		configuration
				.set("hbase.zookeeper.property.maxClientCnxns", properties.getProperty("hbase.zookeeper.property.maxClientCnxns"));
		configuration.set("hadoop.home.dir", properties.getProperty("hadoop.home.dir"));

		properties.getProperty("hdfs.source.input");

		Configuration cfg = HBaseConfiguration.create(configuration);

		admin = new HBaseAdmin(cfg);
		hbaseTemplate = new HbaseTemplate(cfg);
		hBaseDao = new HBaseDao();

		hbaseTemplate.setAutoFlush(true);
		hbaseTemplate.setConfiguration(cfg);
	}



	@SuppressWarnings("unused")
	private void put(String tableName, final String rowkey, final String kType, final String value) {

		try {
			hBaseDao.createTable(admin, tableName);
			LOG.info("start insert data ......");
			hbaseTemplate.execute(tableName, new TableCallback<String>() {
				public String doInTable(HTableInterface table) throws Throwable {
					Put put = hBaseDao.getPut(rowkey, kType, value);
					table.put(put);
					return value;
				}
			});
		} catch (Exception e) {
			LOG.error("=========put(String tableName, final String kType, final String value)=========");
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	private Object getValue(String tableName, String rowkey) throws IOException {

		long ts1 = System.currentTimeMillis();
		Object value = 0;
		HTable table = new HTable(configuration, tableName);
		Get g = new Get(rowkey.getBytes());
		Result rs = table.get(g);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + "  ");// 行号
			System.out.print(new String(kv.getFamily()) + ":");// 列簇名
			System.out.print(new String(kv.getQualifier()) + "  ");// 列名
			System.out.print(kv.getTimestamp() + "  ");// 时间戳
			value = kv.getValue().toString();
		}

		long ts2 = System.currentTimeMillis();
		LOG.debug("========getValue time========" + String.valueOf(ts2 - ts1));
		LOG.debug("========getValue value========" + value);
		return value;
	}

	/**
	 * main 函数
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		System.out.println(HBaseService.config_domain);
		
	}
}
