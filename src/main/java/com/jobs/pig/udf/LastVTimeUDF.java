package com.jobs.pig.udf;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import com.jobs.kpi.constants.Constants;
import com.jobs.kpi.entity.KPIUDF;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [LastVTimeUDF]
 * @Description:  [用于计算退出率是用到的最后一次访问页面,主要目的是去重]
 * 如,
 * 输入格式大致为:"{(11111111,a,ceshi1,pvt=2013-08-18 12:00:00),(11111111,a,ceshi2,pvt=2013-08-18 12:00:05)}"
 * 輸出结果：11111111,a,ceshi2,pvt=2013-08-18 12:00:05
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:26:28]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:26:28]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class LastVTimeUDF extends EvalFunc<String> {

	public static String split(String s) {
		LinkedList<KPIUDF> list = new LinkedList<KPIUDF>();
		s = s.substring(1, s.length() - 1);
		if (s.split("\\),").length <= 1) {
			s = s.substring(1, s.length() - 1);
			return s;
		}
		int start, end;
		boolean open = false;
		KPIUDF kpiudf = null;
		Map<String, String> map = null;
		for (int i = 0; i < s.length(); i++) {
			try {
				start = i;
				for (end = start; end < s.length(); end++, i++) {
					if (s.charAt(end) == '(' || s.charAt(end) == ')')
						open = !open;
					if (s.charAt(end) == ',' && !open) {
						String[] arr = s.substring(start + 1, end - 1).split(Constants.COMMA);
						map = KPIUDF.getPropertyMap(arr);
						kpiudf = KPIUDF.getInstance(map);
						list.add(kpiudf);
						break;
					}
				}
				if (i == s.length() && end > start) {
					String[] arr = s.substring(start + 1, end - 1).split(Constants.COMMA);
					map = KPIUDF.getPropertyMap(arr);
					kpiudf = KPIUDF.getInstance(map);
					list.add(kpiudf);
					
				}
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		
		Collections.sort(list);
		KPIUDF k = list.getLast();
		return k.getDistinctUDFString();
	}

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		try {
			DataBag bag = DataType.toBag(input.get(0));
//			bag.
			
			String val = DataType.toString(input.get(0));
			val = split(val);
			return val;
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}

//	public static void main(String[] args) {
//		DistinctUDF test = new DistinctUDF();
//		String line = "{(11111111,a,ceshi1,ada,sadf,asdfsda,2013-12-02 12:00:34),(11111111,a,ceshi2,ada,sadf,asdfsda,2013-08-18 12:00:30)"
//				+ ",(11111111,a,ceshi3,ada,sadf,asdfsda,2013-12-02 12:00:33)}";
//		String tmp = test.split(line);
//		System.out.println(tmp);
//		
//	}
}
