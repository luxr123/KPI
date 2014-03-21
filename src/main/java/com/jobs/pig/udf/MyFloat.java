
package com.jobs.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [MyFloat]
 * @Description:  [用于计算平均访问时长的float类型数据类]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:31:50]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:31:50]
 * @UpdateRemark: [目前已修改flaot为long]
 * @Version:      [v1.0]
 *
 */
public class MyFloat extends EvalFunc<Long>{

	@Override
	public Long exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0 || input.get(0) == null || "".equals(input.get(0)))
            return null;

        Float d;
        try{
//			d = ((Float) input.get(0)) * 100;
			d = (Float) input.get(0);
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }

//		return (float)(Math.round(d*100))/100; //(这里的100就是2位小数点,如果要其它位,如4位,这里两个100改成10000)
		return (long)(Math.round(d*10000));
	}
	
	@Override
	public Schema outputSchema(Schema input) {
//        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.FLOAT));
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
	}
	
}
