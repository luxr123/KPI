package com.jobs.kpi.mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jobs.kpi.constants.Constants;
import com.jobs.kpi.constants.KPIConstants;
import com.jobs.kpi.entity.KPI;
import com.jobs.kpi.hbase.service.HBaseService;

/**
 * 
 * Simple to Introduction
 * 
 * @ProjectName: [KPI]
 * @Package: [com.jobs.kpi.mapred]
 * @ClassName: [KPIMapRed]
 * @Description: [日志信息进行mapreduce,清洗数据并格式化]
 * @Author: [xiaorui.lu]
 * @CreateDate: [2014年3月21日 下午2:00:10]
 * @UpdateUser: [xiaorui.lu]
 * @UpdateDate: [2014年3月31日 下午2:00:10]
 * @UpdateRemark: [1.引入缓存文件,代码重构]
 * @Version: [v1.0]
 * 
 */
public class KPIMapRed {

	private static String filterRegex = "(?:";
	private static Pattern filterPattern;
	private static HashSet<String> filterRegexs = HBaseService.config_domain.get(Constants.FILTER);

	/**
	 * 得到hbase中的配置信息,取出过滤的正则表达式
	 */
	static {
		for (String s : filterRegexs) {
			filterRegex += s + "|";
		}
		filterRegex = filterRegex.substring(0, filterRegex.length() - 1) + ")";
		filterPattern = Pattern.compile(filterRegex);

	}

	public static class KPIMapper extends Mapper<Object, Text, NullWritable, Text> {
		private Text result = new Text();

		// 多路输出
		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		// 拿到缓存ip文件
		private Path[] localPaths;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

			localPaths = context.getLocalCacheFiles();
			if (null != localPaths)
				Constants.IP_FILE = localPaths[0].toString();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (!filter(line)) {
				KPI kpi = KPI.filterDomain(line);
				if (kpi != null) {
					if (KPIConstants.VG.equals(kpi.getM_job_page())) {
						result.set(kpi.getPageTimeLength());
						multipleOutputs.write(NullWritable.get(), result, "PageTimeLength/");
						// } else if
						// (KPIConstants.VC.equals(kpi.getM_job_page())) {
						// word.set("HeatMap");
						// value.set(kpi.getHeatMapPage());
						// context.write(word, value);
					} else {
						result.set(kpi.getBasicField());
						multipleOutputs.write(NullWritable.get(), result, "BasicField/");
					}
				}
				KPI ads = KPI.filterAdsPage(line);
				if (ads != null) {
					if (KPIConstants.VG.equals(ads.getM_job_page())) {
						result.set(ads.getAdsTimeLength());
						multipleOutputs.write(NullWritable.get(), result, "AdsTimeLength/");
					} else {
						result.set(ads.getAds());
						multipleOutputs.write(NullWritable.get(), result, "Ads/");
					}
				}
				KPI hotLinkPage = KPI.filterHotPage(line);
				if (hotLinkPage != null) {
					result.set(hotLinkPage.getHotLink());
					multipleOutputs.write(NullWritable.get(), result, "HotLink/");
				}
			}

		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}

		private boolean filter(String line) {
			if (filterRegexs.isEmpty())
				return false;
			Matcher m = filterPattern.matcher(line);
			return m.find();
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (remainingArgs.length != 2) {
			System.err.println("Usage: KPIMapRed <in> <out>");
			System.exit(2);
		}

		// HdfsUtil.delete(conf, remainingArgs[remainingArgs.length - 1]);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "KPI_MapReduce");
		job.setJarByClass(KPIMapRed.class);
		job.setMapperClass(KPIMapper.class);
		// MultipleOutputs.addNamedOutput(job,"MOSInt",OutputFormat.class,Text.class,IntWritable.class);
		// MultipleOutputs.addNamedOutput(job,"MOSText",OutputFormat.class,Text.class,IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		// 用于取消mapreduce自动生成的文件，如part-r-00000这个文件
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		job.addCacheFile(new Path(Constants.HDFS_IP_FILE).toUri());

		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
