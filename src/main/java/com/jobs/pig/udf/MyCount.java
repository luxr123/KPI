
package com.jobs.pig.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.pig.udf]
 * @ClassName:    [MyCount]
 * @Description:  [指定为'1'或者'-'时计数器]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:30:53]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:30:53]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class MyCount extends COUNT{
	
	@Override
	public Long exec(Tuple input) throws IOException {
		// input eg, {({(1)}),({(1)}),({(0)})},{({(-)}),({(-)}),({(-)})})
		try {
			DataBag bag = (DataBag) input.get(0);
			Iterator it = bag.iterator();
			long cnt = 0;
			while (it.hasNext()) {
				Tuple t = (Tuple) it.next(); // ({(1)})/wb\new ({(-)})/ref 
				String s = StringUtils.strip(t.toString(), "({})");
				if("1".equals(s) || "-".equals(s))
					cnt++;
			}
			return cnt;
		} catch (ExecException ee) {
			throw ee;
		} catch (Exception e) {
			int errCode = 2106;
			String msg = "Error while computing count in " + this.getClass().getSimpleName();
			throw new ExecException(msg, errCode, PigException.BUG, e);
		}
	}
}

