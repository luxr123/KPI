package com.jobs.kpi.entity;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.jobs.ip.entity.IPLocation;
import com.jobs.kpi.constants.Constants;
import com.jobs.kpi.ip.service.IPSeeker;
import com.jobs.utils.KPIUtil;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.kpi.entity]
 * @ClassName:    [KPI]
 * @Description:  [KPI统计实体类]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:06:18]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:06:18]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class KPI {
	
	private static IPSeeker ipSeeker = IPSeeker.getInstance();

	private String m_remote_ip; 			 		 				 // 记录客户端的ip地址
	private String m_wid; 					 		 				 // 记录客户端cookie中的id地址
	private String m_visit_page; 									 // 记录访问页面
	private String m_status; 						 				 // 记录请求状态；成功是200
	private String m_http_referer; 					 				 // 用来记录从那个页面链接访问过来的
	private String m_domain; 										 // 记录域名
	private String m_job_page;										 // 记录埋点源
	private String m_page_avg_time;									 // 记录页面停留时间
	private String m_page_visit_time;								 // 记录页面访问时间
	private String m_respondents_domain;			 				 // 记录受访域名
	private String m_heat_page;			 				 		 	 // 记录想要统计的页面
	private String m_click_mark;			 				 		 // 记录被统计页面的点击标识
	private String m_is_entry;			 				 		     // 记录是否为入口页面
	private String m_is_new;			 				 		     // 记录新访客
	private String m_adsref;			 				 		     // 记录广告来源
	private String m_adsnew;			 				 		     // 为1表示广告入口, 为0表示旧广告
	private String m_adsid;			 				 		         // 记录广告ID
	private String m_hot;			 				 		         // 记录热击链接
	private String m_wb;			 				 		         // 记录外部链接
	private String m_se;			 				 		         // 记录为搜索引擎
	private String m_kw;			 				 		         // 记录搜索词
	private IPLocation m_ip_location;			 				 	 // 记录ip的地区

	private List<Hot> m_hotList;     				 	 			 // 要统计的链接热击图集合
	
	// kpi单实例
	private static KPI kpiInstance = null;

	private KPI() {}

	/**
	 * @param map  包含kpi属性值的map集合
	 * @return     kpi单实例对象
	 */
	public static KPI getKpiInstance(Map<String, String> map) {
		if (kpiInstance == null) 
			kpiInstance = new KPI();
		kpiInstance.setM_remote_ip(map.get(Constants.REMOTE_IP));
		kpiInstance.setM_wid(map.get(Constants.WID));
		kpiInstance.setM_visit_page(map.get(Constants.VISIT_PAGE));
		kpiInstance.setM_status(map.get(Constants.STATUS));
		kpiInstance.setM_http_referer(map.get(Constants.HTTP_REFERER));
		kpiInstance.setM_domain(map.get(Constants.DOMAIN));
		kpiInstance.setM_job_page(map.get(Constants.JOB_PAGE));
		kpiInstance.setM_page_avg_time(map.get(Constants.PAGE_AVG_TIME));
		kpiInstance.setM_page_visit_time(map.get(Constants.PAGE_VISIT_TIME));
		kpiInstance.setM_respondents_domain(map.get(Constants.RESPONDENTS_DOMAIN));
		kpiInstance.setM_heat_page(map.get(Constants.ACCOUNT_PAGE));
		kpiInstance.setM_click_mark(map.get(Constants.CLICK_MARK));
		kpiInstance.setM_is_entry(map.get(Constants.IS_ENTRY));
		kpiInstance.setM_is_new(map.get(Constants.IS_NEW));
		kpiInstance.setM_adsref(map.get(Constants.ADSREF));
		kpiInstance.setM_adsnew(map.get(Constants.ADSNEW));
		kpiInstance.setM_adsid(map.get(Constants.ADSID));
		kpiInstance.setM_hot(map.get(Constants.HOT));
		kpiInstance.setM_wb(map.get(Constants.WB));
		kpiInstance.setM_se(map.get(Constants.SE));
		kpiInstance.setM_kw(map.get(Constants.KW));
		kpiInstance.setM_ip_location(ipSeeker.getIPLocation(kpiInstance.getM_remote_ip()));
		return kpiInstance;
	}

	/**
	 * 域名下的基本信息统计分析过滤，如pv，uv，ip，访问时长，跳出率，退出率等
	 * @param line    每行日志记录信息
	 * @return        kpi的实体
	 */
	public static KPI filterDomain(String line) {
		KPI kpi = KPIUtil.parser(line);
		if (kpi == null) 
			return null;
//		if (KPIConstants.VC.equals(kpi.getM_job_page()))
//			return kpi;
		String dom = kpi.getM_domain();
		if (KPIUtil.domainSet.contains(dom))
			return kpi;
		return null;
	}
	
	/**
	 * 广告的基本信息统计分析过滤
	 * @param line    每行日志记录信息
	 * @return        kpi的实体
	 */
	public static KPI filterAdsPage(String line) {
		KPI kpi = KPIUtil.parserAds(line);
		if (kpi == null) 
			return null;
		return kpi;
	}
	
	/**
	 * 热力图的基本信息统计分析过滤
	 * @param line    每行日志记录信息
	 * @return        kpi的实体
	 */
	public static KPI filterHotPage(String line) {
		KPI kpi = KPIUtil.parserHot(line);
		if (kpi == null)
			return null;
		return kpi;
	}
	
	/**
	 * 目的:得到kpi基本字段信息
	 * @return  以'`'为分隔符的基本字段字符串信息
	 */
	public String getBasicField() {
		return concatStr(m_remote_ip, m_wid, m_visit_page, m_http_referer, m_domain, m_respondents_domain, m_is_entry, m_is_new, m_se,
				m_wb, m_kw, m_page_visit_time , m_ip_location.getIPInfo());
	}

	/**
	 * 目的:得到统计平均时长的基本字段信息
	 * @return    以'`'为分隔符的平均时长等字符串信息
	 */
	public String getPageTimeLength() {
		return concatStr(m_visit_page, m_http_referer, m_domain, m_page_avg_time, m_respondents_domain, m_is_entry, m_is_new, m_se,
				m_wb, m_kw, m_ip_location.getIPInfo());
	}

	/**
	 * 目的:得到有关热力图埋点的基本字段信息
	 * @return    以'`'为分隔符的热力图埋点相关的字符串信息
	 */
	public String getHeatMapPage() {
		return concatStr(m_heat_page, m_click_mark);
	}

	/**
	 * 目的:得到有关广告的基本字段信息
	 * @return    以'`'为分隔符的广告相关的字符串信息
	 */
	public String getAds() {
		return concatStr(m_remote_ip, m_wid, m_domain, m_is_new, m_adsref, m_adsnew, m_adsid);
	}

	/**
	 * 目的:得到有关广告平均时长的基本字段信息
	 * @return    以'`'为分隔符的广告平均时长相关的字符串信息
	 */
	public String getAdsTimeLength() {
		return concatStr(m_domain, m_adsid, m_is_new, m_page_avg_time);
	}

	/**
	 * 目的:得到有关热力图坐标的基本字段信息
	 * @return    以'`'为分隔符的热力图坐标相关的字符串信息
	 */
	public String getHotLink() {
		List<Hot> hots = kpiInstance.getM_hotList();
		StringBuilder sb = new StringBuilder();
		for (Hot hot : hots) {
			sb.append(m_domain).append(Constants.MARK).append(m_visit_page).append(Constants.MARK).append(hot.toString()).append("\n");
		}
		return StringUtils.trim(sb.toString());
	}

	/**
	 * 拼接字符串
	 * @param args
	 * @return
	 */
	public String concatStr(String... args) {
		StringBuilder sb = new StringBuilder();
		for (String arg : args) {
			sb.append(arg).append(Constants.MARK);
		}
		return sb.substring(0, sb.length() - 1);
	}
	
	public String getM_remote_ip() {
		return m_remote_ip;
	}

	public void setM_remote_ip(String m_remote_ip) {
		this.m_remote_ip = m_remote_ip;
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

	public String getM_status() {
		return m_status;
	}

	public void setM_status(String m_status) {
		this.m_status = m_status;
	}

	public String getM_http_referer() {
		return m_http_referer;
	}

	public void setM_http_referer(String m_http_referer) {
		this.m_http_referer = m_http_referer;
	}

	public String getM_domain() {
		return m_domain;
	}

	public void setM_domain(String m_domain) {
		this.m_domain = m_domain;
	}

	public static KPI getKpiInstance() {
		return kpiInstance;
	}

	public static void setKpiInstance(KPI kpiInstance) {
		KPI.kpiInstance = kpiInstance;
	}

	public String getM_job_page() {
		return m_job_page;
	}

	public void setM_job_page(String m_job_page) {
		this.m_job_page = m_job_page;
	}
	

	public String getM_page_avg_time() {
		return m_page_avg_time;
	}

	public void setM_page_avg_time(String m_page_avg_time) {
		this.m_page_avg_time = m_page_avg_time;
	}
	
	public String getM_page_visit_time() {
		return m_page_visit_time;
	}

	public void setM_page_visit_time(String m_page_visit_time) {
		this.m_page_visit_time = m_page_visit_time;
	}
	
	public String getM_respondents_domain() {
		return m_respondents_domain;
	}

	public void setM_respondents_domain(String m_respondents_domain) {
		this.m_respondents_domain = m_respondents_domain;
	}

	public String getM_heat_page() {
		return m_heat_page;
	}

	public void setM_heat_page(String m_heat_page) {
		this.m_heat_page = m_heat_page;
	}

	public String getM_click_mark() {
		return m_click_mark;
	}

	public void setM_click_mark(String m_click_mark) {
		this.m_click_mark = m_click_mark;
	}

	public String getM_is_entry() {
		return m_is_entry;
	}

	public void setM_is_entry(String m_is_entry) {
		this.m_is_entry = m_is_entry;
	}

	public String getM_is_new() {
		return m_is_new;
	}

	public void setM_is_new(String m_is_new) {
		this.m_is_new = m_is_new;
	}

	public String getM_adsref() {
		return m_adsref;
	}

	public void setM_adsref(String m_adsref) {
		this.m_adsref = m_adsref;
	}

	public String getM_adsnew() {
		return m_adsnew;
	}

	public void setM_adsnew(String m_adsnew) {
		this.m_adsnew = m_adsnew;
	}

	public String getM_adsid() {
		return m_adsid;
	}

	public void setM_adsid(String m_adsid) {
		this.m_adsid = m_adsid;
	}

	public String getM_hot() {
		return m_hot;
	}

	public void setM_hot(String m_hot) {
		this.m_hot = m_hot;
	}

	public List<Hot> getM_hotList() {
		return m_hotList;
	}

	public void setM_hotList(List<Hot> m_hotList) {
		this.m_hotList = m_hotList;
	}

	public String getM_wb() {
		return m_wb;
	}

	public void setM_wb(String m_wb) {
		this.m_wb = m_wb;
	}

	public String getM_se() {
		return m_se;
	}

	public void setM_se(String m_se) {
		this.m_se = m_se;
	}

	public String getM_kw() {
		return m_kw;
	}

	public void setM_kw(String m_kw) {
		this.m_kw = m_kw;
	}

	public IPLocation getM_ip_location() {
		return m_ip_location;
	}

	public void setM_ip_location(IPLocation m_ip_location) {
		this.m_ip_location = m_ip_location;
	}
	
	public static void main(String[] args) throws Exception {
//		String s = "10.100.50.10002014-02-25 16:00:00GET/5a.gif?wb=1&cd=24&ck=1&la=zh-CN&ws=1440x900&"
//				+ "re=http%3a%2f%2fmy.51job.com%2fmy%2f100000%2fMy_Pmc.php%3fid%3d10000000&"
//				+ "u=http%3a%2f%2fmy.51job.com%2fmy%2f100000%2fMy_Pmc.php%3fid%3d10000000&"
//				+ "id=www&st=1393315196986&lnt=3261&cn=0&hot=[{x:272.5,y:615,t:b}]2000http://my.51job.com/my/100000/My_Pmc.php?id=10000000Mozilla/5.0 (Windows NT 6.1; rv:26.0) Gecko/20100101 Firefox/26.0wid=5ef03619a7eb3d2dba8e615bbfd6b0277a9aa126; enter=0";
		String s = "1.25.96.002014-01-07 09:15:06GET/5a.gif?cd=24&ck=1&la=zh-CN&ws=1440x900&re=&id=xy&st=1389057306113&u=http://192.168.159.100/circle/hot_circle.php2000http://192.168.159.100/circle/hot_circle.phpMozilla/5.0 (Windows NT 6.1; rv:24.0) Gecko/20100101 Firefox/24.0wid=5ef03619a7eb3d2dba8e615bbfd6b0277a9aa126; enter=0";
		
		KPI kpi = KPI.filterDomain(s);
		System.out.println(kpi.getBasicField());
	}

}
