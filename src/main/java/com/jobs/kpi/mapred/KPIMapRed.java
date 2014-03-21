package com.jobs.kpi.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import com.jobs.kpi.constants.Constants;
import com.jobs.kpi.constants.KPIConstants;
import com.jobs.kpi.entity.KPI;
import com.jobs.kpi.hbase.service.HBaseService;
import com.jobs.utils.HdfsUtil;


/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.kpi.mapred]
 * @ClassName:    [KPIMapRed]
 * @Description:  [日志信息进行mapreduce,清晰数据并格式化]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:00:10]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:00:10]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class KPIMapRed extends MultipleTextOutputFormat<Text, Text> {

	private static String filterRegex = "(?:";
	private static Pattern filterPattern;

	/**
	 * 得到hbase中的配置信息,取出url替换的正则表达式
	 */
	static {
		for (String s : HBaseService.config_domain.get(Constants.FILTER)) {
			filterRegex += s + "|";
		}
		filterRegex = filterRegex.substring(0, filterRegex.length() - 1) + ")";
		filterPattern = Pattern.compile(filterRegex);

	}

	public static class KPIDomainMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text value = new Text();

		public void map(Object k, Text v, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = v.toString();
			if (!filter(line)) {
				KPI kpi = KPI.filterDomain(line);
				if (kpi != null) {
					if (KPIConstants.VG.equals(kpi.getM_job_page())) {
						word.set("PageTimeLength");
						value.set(kpi.getPageTimeLength());
						output.collect(word, value);
					} else if (KPIConstants.VC.equals(kpi.getM_job_page())) {
						word.set("HeatMap");
						value.set(kpi.getHeatMapPage());
						output.collect(word, value);
					} else {
						word.set("BasicField");
						value.set(kpi.getBasicField());
						output.collect(word, value);
					}
				}
				KPI ads = KPI.filterAdsPage(line);
				if (ads != null) {
					if (KPIConstants.VG.equals(ads.getM_job_page())) {
						word.set("AdsTimeLength");
						value.set(ads.getAdsTimeLength());
						output.collect(word, value);
					} else {
						word.set("Ads");
						value.set(ads.getAds());
						output.collect(word, value);
					}
				}
				KPI hotLinkPage = KPI.filterHotPage(line);
				if (hotLinkPage != null) {
					word.set("HotLink");
					value.set(hotLinkPage.getHotLink());
					output.collect(word, value);
				}
			}
		}

		private boolean filter(String line) {
			Matcher m = filterPattern.matcher(line);
			return m.find();
		}

	}

	public static class KPIDomainReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		private Text word = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				word.set(key.toString());
				result = values.next();
				output.collect(word, result);
			}
		}
	}

	/**
	 * Use they key as part of the path for the final output file. 指定了作业输出的存储位置
	 */
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value, String leaf) {
		return new Path(key.toString(), leaf).toString();
	}

	/**
	 * When actually writing the data, discard the key since it is already in
	 * the file path.
	 */
	@Override
	protected Text generateActualKey(Text key, Text value) {
		return null;
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(KPIMapRed.class);
		HdfsUtil.delete(conf, args[args.length - 1]);
		conf.setJobName("KPI_MapReduce");
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(KPIDomainMapper.class);
		conf.setCombinerClass(KPIDomainReducer.class);
		conf.setReducerClass(KPIDomainReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(KPIMapRed.class);
		conf.setNumReduceTasks(10);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		System.exit(0);

	}

}
