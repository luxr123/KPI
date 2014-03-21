package com.jobs.pig.udf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.jobs.kpi.constants.Constants;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [ConcatUDF]
 * @Description:  [字符串拼接udf,目前是默认按照'\x1A'连接]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:19:58]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:19:58]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class ConcatUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		try {
			int size = input.size();
			if (input == null || size == 0) {
				return null;
			}
			// param 传入的参数
			String param0 = null;
			String param1 = null;
			String param2 = null;
			String param3 = null;
			String param4 = null;
			String param5 = null;
			switch (size) {
			case 2: // ($time, domain) (page, ref)
				param0 = formatData(input.get(0));
				param1 = formatData(input.get(1));
				return getStr(param0, param1);
			case 3: // ($time, domain, page) ($time, count, page_ref)
				param0 = formatData(input.get(0));
				Object arg  = input.get(1);
				if (arg instanceof Long || arg instanceof Integer)
					param1 = Long.toBinaryString(-(DataType.toLong(arg)));
				else
					param1 = formatData(arg);
				param2 = formatData(input.get(2));
				return getStr(param0, param1, param2);
			case 4: // $time, page, x, y
				param0 = formatData(input.get(0));
				Object arg1  = input.get(1);
				if (arg1 instanceof Long || arg1 instanceof Integer)
					param1 = Long.toBinaryString(-(DataType.toLong(arg1)));
				else
					param1 = formatData(arg1);
				param2 = formatData(input.get(2));
				param3 = formatData(input.get(3));
				return getStr(param0, param1, param2, param3);
			case 5: // $time, page, x, y
				param0 = formatData(input.get(0));
				param1 = formatData(input.get(1));
				param2 = formatData(input.get(2));
				param3 = formatData(input.get(3));
				param4 = formatData(input.get(4));
				return getStr(param0, param1, param2, param3, param4);
			case 6: // $time, page, x, y,..
				param0 = formatData(input.get(0));
				param1 = formatData(input.get(1));
				param2 = formatData(input.get(2));
				param3 = formatData(input.get(3));
				param4 = formatData(input.get(4));
				param5 = formatData(input.get(5));
				return getStr(param0, param1, param2, param3, param4, param5);
			default:
				return null;
			}
		} catch (ExecException exp) {
			throw exp;
		}
	}
	
	/**
	 * @param object
	 * @return  去除数据的关系后的字符串
	 */
	public String formatData(Object object) {
		if(object instanceof Tuple) 
			return StringUtils.strip(object.toString(), "()");
		else
			return object==null ? "" : object.toString();
		
	}

	/**
	 * 
	 * @param args
	 * @return 拼接后的字符串
	 */
	public String getStr(String... args) {
		StringBuilder sb = new StringBuilder();
		for (String arg : args) {
			sb.append(arg).append(Constants.LOG_DELIMITER);
		}
		return sb.substring(0, sb.length() - 1);
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
	}
	
}
