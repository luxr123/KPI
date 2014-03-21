package com.jobs.kpi.hbase.dao;

import java.io.IOException;

/**
 * HBase 的数据访问层
 */
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jobs.kpi.constants.Constants;
/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.kpi.hbase.dao]
 * @ClassName:    [HBaseDao]
 * @Description:  [操作hbase的DAO层]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:14:32]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:14:32]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class HBaseDao {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseDao.class);				// log

	/**
	 * 
	 * @param rowkey
	 * @param kType
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public Put getPut(String rowkey, String kType, String value) throws Exception {
		Put put = null;
		put = new Put(Bytes.toBytes(rowkey));
		put.add(Constants.CF_BYTES, Bytes.toBytes(kType), Bytes.toBytes(value));
		return put;
	}

	/**
	 * 
	 * @param admin
	 * @param tableName
	 * @throws IOException
	 */
	public void createTable(HBaseAdmin admin, String tableName) throws IOException {
		if (!admin.tableExists(tableName)) {
			LOG.info("start create table ......");
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor(Constants.CF));
			admin.createTable(tableDescriptor);
		}

	}

	/**
	 * 
	 * @param admin
	 * @param tableName
	 * @throws IOException
	 */
	public void deleteTable(HBaseAdmin admin, String tableName) throws IOException {
		LOG.info("start delete table ......");
		if (admin.tableExists(tableName)) {
			if (admin.isTableEnabled(tableName)) {
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
		}
	}
}
