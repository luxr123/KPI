
package com.jobs.pig.udf;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [FilterCountUDF]
 * @Description:  [根据输入条件过滤类]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:23:14]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:23:14]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class FilterCountUDF extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
		try {
			int size = input.size();
			if (input == null || size == 0) {
				return false;
			}
			DataBag bag = (DataBag) input.get(0);
			switch (size) {
			case 2:
				int count = DataType.toInteger(input.get(1));
				return bag.size() == count;
			case 3:
				int begin = DataType.toInteger(input.get(1));
				int end = DataType.toInteger(input.get(2));
				return (bag.size() >= begin) && (bag.size() <= end);
			default:
				int errCode = 10001;
				String msg = "Cannot test a " + DataType.findTypeName(input.get(0)) + " for its size.";
				throw new ExecException(msg, errCode, PigException.BUG);
			}
		} catch (ExecException exp) {
			throw exp;
		}
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.BOOLEAN));
	}
	
}
