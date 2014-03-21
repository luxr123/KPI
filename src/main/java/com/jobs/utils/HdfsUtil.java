package com.jobs.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.utils]
 * @ClassName:    [HdfsUtil]
 * @Description:  [hdfs文件操作工具集]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:37:25]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:37:25]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public final class HdfsUtil {

	/**
	 * hdfs文件删除操作
	 * @param conf
	 * @param paths
	 * @throws IOException
	 */
	public static void delete(Configuration conf, String... paths) throws IOException {
		if (conf == null) {
			conf = new Configuration();
		}

		for (String p : paths) {
			String path = Config.HDFS_ROOT + p;
			FileSystem fs = FileSystem.get(URI.create(p), conf);
			fs.delete(new Path(path), true);
		}
	}

	/**
	 * hdfs文件写操作
	 * @param fileName
	 * @param list
	 */
	public static void write(String fileName, List<String> list) {

		String path = fileName;

		try {

			File f = new File(path);

			if (!f.exists()) {
				f.createNewFile();
			}

			/**
			 * 追写文件 需要设置hdfs-site.xml
			 */
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "UTF-8"));
			for (String s : list) {
				bw.write(s);
				bw.newLine();
			}
			bw.flush();
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
