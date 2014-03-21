
package com.jobs.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.jobs.kpi.constants.Constants;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [UnConcatUDF]
 * @Description:  [对ConcatUDF的的加压操作,得到域名]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:35:07]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:35:07]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class UnConcatUDF extends EvalFunc<String> {
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		String bag = ((DataBag) input.get(0)).toString(); // eg, {(20140109xygoogle.com),(20140109xybaidu.com)}
		return bag.substring(2, bag.indexOf(Constants.LOG_DELIMITER, bag.indexOf(Constants.LOG_DELIMITER) + 1));
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
	}

}
