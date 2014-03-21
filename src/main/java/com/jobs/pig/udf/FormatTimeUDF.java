package com.jobs.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.jobs.utils.TimestampUtils;
/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [FormatTimeUDF]
 * @Description:  [时间格式化udf类]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:25:29]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:25:29]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class FormatTimeUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		try {
			long microTimestamp = Math.round(Double.parseDouble(input.get(0).toString()));
			return TimestampUtils.formatDuring(microTimestamp);
		} catch (NumberFormatException e) {
			log.error("格式转化错误...." + input.size() + "illegal !!");
		} catch (Exception exp) {
			log.error("input may be null or anything else, please cheak it " + exp);
		}
		return null;
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
	}

	/**(限定了 输入的格式)
	 * EvalFunc为此提供了 getArgToFuncMapping()方法。我们可以重载这个方法来告诉Pig第一个字段应 该是长整型。
	 * 这个方法返回一个FuncSpec对象，后者对应于传递给exec()方法的那个元组的
	 * 每个字段。在这个例子里，只有一个字段。我们构造一个匿名FieldSchema(因为
	 * Pig在进行类型转换时忽略其名称，因此其名称以null传递)。该类型使用Pig的 DataType类的常量LONG进行指定。
	 * 
	 * 使用这个修改后的函数，Pig将尝试把传递给函数的参数转换成整型。如果无法转 换这个字段，则把这个字段传递为null。如果字段为null,
	 * exec()方法返回的 结果总是"-"。在这个应用中,因为我们想要在过滤掉其质ft字段包含无法理解 的记录，因此这样做很合适。
	 * 
	 * 如果计算函数要处理一个包(bag),可能需要另外实现Pig
	 * 的Algebraic或Accumulator接口，以提髙以块(chunk)方式处理包的效率。
	 */
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
		funcSpecs.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));
		funcSpecs.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.LONG))));
		funcSpecs.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.FLOAT))));
		funcSpecs.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.DOUBLE))));

		return funcSpecs;
	}

}
