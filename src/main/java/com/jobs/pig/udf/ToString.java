
package com.jobs.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [ToString]
 * @Description:  [用于将bag转化成chararray]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:34:45]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:34:45]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class ToString extends EvalFunc<String> {
	@Override
	public String exec(Tuple input) throws IOException {
		try {
			int size = input.size();
			if (input == null || size == 0) {
				return null;
			}
			Object field = input.get(0);
			switch (DataType.findType(field)) {
			case DataType.NULL:
			case DataType.BAG:
				DataBag values = (DataBag)field;
				return values.iterator().next().get(0).toString();
			case DataType.BOOLEAN:
			case DataType.INTEGER:
			case DataType.LONG:
			case DataType.FLOAT:
			case DataType.DOUBLE:
			case DataType.BYTEARRAY:
			case DataType.CHARARRAY:
			case DataType.BYTE:
			case DataType.MAP:
			case DataType.TUPLE:
			default:
				throw new RuntimeException("Unknown datatype " + DataType.findType(field));
			}
		} catch (Exception exp) {
			log.debug(ToString.class.toString() + "get the" + exp);
			return null;
		}
	}
}
