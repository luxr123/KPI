
package com.jobs.pig.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * User: xiaorui.lu
 * Date: 2014年1月8日 下午10:33:28
 */
public class testUDF {
	private static TupleFactory mTupleFactory = TupleFactory.getInstance();
	private static BagFactory mBagFactory = BagFactory.getInstance();
	public static void main(String[] args) throws ExecException {
		Tuple tuple = mTupleFactory.newTuple();
		tuple.append(1);
		List<Tuple> list = new ArrayList<Tuple>();
		list.add(tuple);
		DataBag bag = mBagFactory.newDefaultBag(list);
		Tuple tuple2 = mTupleFactory.newTuple();
		tuple2.append(bag);
		
		System.out.println(tuple);
		System.out.println(bag);
		System.out.println(tuple2);
		System.out.println(StringUtils.strip(tuple2.toString(), "({})"));
	}
}

