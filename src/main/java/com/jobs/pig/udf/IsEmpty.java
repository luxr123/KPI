
package com.jobs.pig.udf;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [IsEmpty]
 * @Description:  [判断bag是否为空的过滤器]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:26:09]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:26:09]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class IsEmpty extends FilterFunc {
	@Override
    public Boolean exec(Tuple input) throws IOException {
        try {
            Object values = input.get(0);        
            if (values instanceof DataBag)
                return StringUtils.isBlank(StringUtils.strip(values.toString(), "{()}"));
            else if (values instanceof Map)
                return ((Map)values).size() == 0;
            else {
                int errCode = 2102;
                String msg = "Cannot test a " +
                DataType.findTypeName(values) + " for emptiness.";
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        } catch (ExecException ee) {
            throw ee;
        }
    }
}

