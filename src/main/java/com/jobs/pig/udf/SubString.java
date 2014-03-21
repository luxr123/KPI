
package com.jobs.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [SubString]
 * @Description:  [用于区别搜索引擎,如,www,3g,m等,在进行字符串的截取得到一级域名]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:33:11]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:33:11]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class SubString extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		try {
			int size = input.size();
			if (input == null || size == 0)
				return null;
			String source = (String)input.get(0);
			if(source.matches("^(www\\.).*"))
				return source.substring(4);
			else if(source.matches("^(m\\.).*"))
				return source.substring(2);
			else if(source.matches("^(3g\\.).*"))
				return source.substring(3);
			else if(source.matches("^(wap\\.).*"))
				return source.substring(4);
			else if(source.matches("^(search\\.).*"))
				return source.substring(7);
			else if(source.matches("^(http://).*"))
				return source.substring(0, source.indexOf("/", "http://".length()));
			else
				return source;
		} catch (ExecException exp) {
			log.debug("Unknown domain site " + input.get(0));
			log.debug(exp.getDetailedMessage());
			return null;
		}
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("subString", DataType.CHARARRAY));
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
    
}

