package com.jobs.kpi.entity;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.jobs.kpi.constants.Constants;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.kpi.entity]
 * @ClassName:    [KPIUDF]
 * @Description:  [pig udf 的kpi实体比较类 by page_visit_time]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:10:24]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:10:24]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class KPIUDF implements Comparable<KPIUDF>{

	private String m_wid; 					 // 记录客户端cookie中的id地址
	private String m_visit_page; 			 // 记录访问页面
	private String m_domain; 				 // 记录域名
	private String m_page_visit_time;		 // 记录页面访问时间

	public static KPIUDF getInstance(Map<String, String> map) {
		KPIUDF kpiudf = new KPIUDF();
		kpiudf.setM_wid(map.get(Constants.WID));
		kpiudf.setM_visit_page(map.get(Constants.VISIT_PAGE));
		kpiudf.setM_domain(map.get(Constants.DOMAIN));
		kpiudf.setM_page_visit_time(map.get(Constants.PAGE_VISIT_TIME));
		return kpiudf;
	}
	
	public static Map<String, String> getPropertyMap(String[] arr) throws UnsupportedEncodingException {
		Map<String, String> map = new HashMap<String, String>();
		map.put(Constants.WID, arr[1]);
		map.put(Constants.VISIT_PAGE, arr[2]);
		map.put(Constants.DOMAIN, arr[4]);
		map.put(Constants.PAGE_VISIT_TIME, arr[6]);
		return map;
	}

	public String getM_wid() {
		return m_wid;
	}

	public void setM_wid(String m_wid) {
		this.m_wid = m_wid;
	}

	public String getM_visit_page() {
		return m_visit_page;
	}

	public void setM_visit_page(String m_visit_page) {
		this.m_visit_page = m_visit_page;
	}

	public String getM_domain() {
		return m_domain;
	}

	public void setM_domain(String m_domain) {
		this.m_domain = m_domain;
	}

	public String getM_page_visit_time() {
		return m_page_visit_time;
	}

	public void setM_page_visit_time(String m_page_visit_time) {
		this.m_page_visit_time = m_page_visit_time;
	}
	
	public String getDistinctUDFString() {
		return m_wid + "," + m_visit_page + "," + m_domain + "," + m_page_visit_time;
	}
	
	@Override
	public String toString() {
		return "KPIUDF [m_wid=" + m_wid + ", m_visit_page=" + m_visit_page + ", m_domain=" + m_domain + ", m_page_visit_time="
				+ m_page_visit_time + "]";
	}

	@Override
	public int compareTo(KPIUDF obj) {
		return this.m_page_visit_time.compareTo(obj.m_page_visit_time);
	}
	
	/*public static void main(String[] args) throws ParseException {
		KPIUDF t1 = new KPIUDF();
		t1.setM_domain("d1");
		t1.setM_page_visit_time("2013-12-02 17:05:30");
		KPIUDF t2 = new KPIUDF();
		t2.setM_domain("d2");
		t2.setM_page_visit_time("2013-12-02 17:05:25");
		LinkedList<KPIUDF> list = new LinkedList<KPIUDF>();
		list.add(t1);
		list.add(t2);
		Collections.sort(list);
		for (KPIUDF k : list){
			System.out.println(k.toString());
		}
	}*/
}
