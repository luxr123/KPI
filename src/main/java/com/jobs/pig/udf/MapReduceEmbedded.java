package com.jobs.pig.udf;

import java.io.IOException;

import org.apache.pig.PigServer;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [MapReduceEmbedded]
 * @Description:  [java操作pig的api实例,暂不作项目运作]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:29:57]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:29:57]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class MapReduceEmbedded {
	public static void main(String[] args) {
		try {
			PigServer ps = new PigServer("local");// local/MapReduce 模式
			runQuery(ps, "/home/hadoop/Desktop/test/pig/a.txt", "local.embedded.out");
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void runQuery(PigServer ps, String inputFile, String outputFile) throws IOException {
		ps.registerQuery("A = LOAD '" + inputFile
				+ "' USING PigStorage(',') AS (col1:chararray,col2:chararray,col3:chararray,col4:chararray);");
		ps.registerQuery("B = FOREACH A GENERATE col1,col2;");
		//ps.store("B", outputFile);
//		ps.registerQuery("dump A;");
	}
}
