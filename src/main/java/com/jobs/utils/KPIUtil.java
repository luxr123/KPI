package com.jobs.utils;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.jobs.kpi.constants.Constants;
import com.jobs.kpi.constants.KPIConstants;
import com.jobs.kpi.entity.Hot;
import com.jobs.kpi.entity.KPI;
import com.jobs.kpi.hbase.service.HBaseService;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.utils]
 * @ClassName:    [KPIUtil]
 * @Description:  [KPI统计的工具类]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午3:03:34]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午3:03:34]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public final class KPIUtil {

	private static final Log LOG = LogFactory.getLog(KPIUtil.class);

	// hbase 中的配置信息
	private static Map<String, HashSet<String>> configMap = HBaseService.config_domain;

	// 要统计的域名集合
	public static Set<String> domainSet = configMap.get(Constants.DOMAIN);

	private static String[] replaceArr;
	private static String replaceRegex;
	private static String replacement;
	private static Pattern replacePattern;

	private static KPI kpi;

	/**
	 * 对日志基本信息格式的解析
	 * @param line
	 * @return
	 */

	public static KPI parser(String line) {
		try {
			Map<String, String> map = getPropertyMap(line);
			if (map != null)
				kpi = KPI.getKpiInstance(map);
			else
				return null;
			// 解析关闭页面时的访问时长不为空
			boolean b1 = KPIConstants.VG.equals(kpi.getM_job_page()) && StringUtils.isBlank(kpi.getM_page_avg_time());
			// 解析埋点统计不为空
			boolean b2 = KPIConstants.VC.equals(kpi.getM_job_page())
					&& (StringUtils.isBlank(kpi.getM_heat_page()) || StringUtils.isBlank(kpi.getM_click_mark()));
			if (b1 || b2)
				return null;
		} catch (Exception e) {
			LOG.error("parser:" + e);
			return null;
		}
		return kpi;
	}

	/**
	 * 对日志中广告的解析
	 * 
	 * @param line
	 * @return
	 */

	public static KPI parserAds(String line) {
		try {
			Map<String, String> map = getPropertyMap(line);
			if (map != null && StringUtils.isNotBlank(map.get(Constants.ADSID))) // 广告id不为空
				kpi = KPI.getKpiInstance(map);
			else
				return null;
		} catch (Exception e) {
			LOG.error("parserAds:" + e);
			return null;
		}
		return kpi;
	}

	/**
	 * 对日志中热力点击链接的解析
	 * 
	 * @param line
	 * @return
	 */

	public static KPI parserHot(String line) {
		try {
			Map<String, String> map = getPropertyMap(line);
			if (map != null)
				kpi = KPI.getKpiInstance(map);
			else
				return null;
			String _tmp_hot = kpi.getM_hot();
			if (StringUtils.isBlank(_tmp_hot))
				return null;
			_tmp_hot = new org.json.JSONArray(_tmp_hot).toString();
			kpi.setM_hotList(JSONArray.parseArray(_tmp_hot, Hot.class));
		} catch (Exception e) {
			LOG.error("parserHot:" + e);
			return null;
		}
		return kpi;
	}

	/**
	 * 得到kpi字段信息map集合
	 * @param line    日志行记录信息
	 * @return        kpi字段信息map集合
	 * @throws Exception
	 */
	public static Map<String, String> getPropertyMap(String line) throws Exception {
		String[] arr = StringUtils.split(line, Constants.LOG_DELIMITER);
		if (arr.length != Constants.KPILENGTH)
			return null;
		String _tmp_ip = arr[0].trim();
		String _tmp_jobpage = StringUtils.strip(arr[4], "/");
		String _tmp_status = StringUtils.strip(arr[6]);
		if (KPIConstants.VEJS.equals(_tmp_jobpage) || Integer.parseInt(_tmp_status) >= 400)
			return null;

		Map<String, String> map = new HashMap<String, String>();
		String[] _arr_cookie = arr[10].split(";");
		String _tmp_wid = null;
		String _tmp_new = "";
		String _tmp_enter = "";
		for (String s : _arr_cookie) {
			if (s.contains("wid"))
				_tmp_wid = s.substring(s.indexOf("=") + 1);
			if (s.contains("newer"))
				_tmp_new = s.substring(s.indexOf("=") + 1);
			if (s.contains("enter"))
				_tmp_enter = s.substring(s.indexOf("=") + 1);
		}
		String _tmp_query_str = StringUtils.strip(arr[5], "?");
		String _tmp_ref = null;
		String _tmp_dom = null;
		String _tmp_lnt = "";
		String _tmp_account_page = "";
		String _tmp_click_mark = "";
		String _tmp_adsref = null;
		String _tmp_adsnew = "";
		String _tmp_adsid = "";
		String _tmp_hot = "";
		String _tmp_wb = "0";
		String _tmp_se = "";
		String _tmp_kw = null;
		String _tmp_visit_page = "";
		String _tmp_respondents_domain = "";

		if (KPIConstants.VC.equals(_tmp_jobpage)) {
			for (String s : _tmp_query_str.split(Constants.AND)) {
				if (s.contains(KPIConstants.CLICK))
					_tmp_click_mark = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.contains(KPIConstants.U))
					_tmp_account_page = s.substring(s.indexOf(Constants.EQUAL) + 1);
			}
			_tmp_account_page = StringUtils.isBlank(_tmp_account_page) ? "-" : URLDecoder.decode(_tmp_account_page, "UTF-8");
		} else {
			for (String s : _tmp_query_str.split(Constants.AND)) {
				if (s.matches("(?:^(" + KPIConstants.RE + ")).*")) // ?:^(re=).*
					_tmp_ref = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.ID + ")).*"))
					_tmp_dom = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.LNT + ")).*"))
					_tmp_lnt = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.U + ")).*"))
					_tmp_visit_page = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.ADSFROM + ")).*"))
					_tmp_adsref = StringUtils.strip(URLDecoder.decode(s.substring(s.indexOf(Constants.EQUAL) + 1), "UTF-8"), "/");
				else if (s.matches("(?:^(" + KPIConstants.ADSNEW + ")).*"))
					_tmp_adsnew = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.ADSNUM + ")).*"))
					_tmp_adsid = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.HOT + ")).*"))
					_tmp_hot = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.WB + ")).*"))
					_tmp_wb = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.SE + ")).*"))
					_tmp_se = s.substring(s.indexOf(Constants.EQUAL) + 1);
				else if (s.matches("(?:^(" + KPIConstants.KW + ")).*"))
					_tmp_kw = s.substring(s.indexOf(Constants.EQUAL) + 1);
			}
			if (StringUtils.isBlank(_tmp_visit_page)) // 访问页面不能为空
				return null;
			_tmp_visit_page = URLDecoder.decode(_tmp_visit_page, "UTF-8");
			_tmp_respondents_domain = StringUtils.strip(_tmp_visit_page, "http://").split("/")[0];
			_tmp_ref = StringUtils.isBlank(_tmp_ref) ? "-" : URLDecoder.decode(_tmp_ref, "UTF-8");
		}

		if (StringUtils.isBlank(_tmp_dom))
			_tmp_dom = "-";
		if (StringUtils.isBlank(_tmp_respondents_domain))
			_tmp_respondents_domain = "-";
		if (StringUtils.isBlank(_tmp_wid))
			_tmp_wid = "-";
		_tmp_adsref = StringUtils.isBlank(_tmp_adsref) ? "-" : URLDecoder.decode(_tmp_adsref, "UTF-8");
		if (StringUtils.isBlank(_tmp_kw))
			_tmp_kw = "-";

		map.put(Constants.REMOTE_IP, _tmp_ip);
		map.put(Constants.WID, _tmp_wid);
		map.put(Constants.VISIT_PAGE, replaceRegx(_tmp_visit_page).split("\\?")[0]);
		map.put(Constants.STATUS, _tmp_status);
		map.put(Constants.HTTP_REFERER, replaceRegx(_tmp_ref).split("\\?")[0]);
		map.put(Constants.DOMAIN, _tmp_dom);
		map.put(Constants.JOB_PAGE, _tmp_jobpage);
		map.put(Constants.PAGE_AVG_TIME, _tmp_lnt);
		map.put(Constants.PAGE_VISIT_TIME, arr[2]);
		map.put(Constants.RESPONDENTS_DOMAIN, _tmp_respondents_domain);
		map.put(Constants.ACCOUNT_PAGE, replaceRegx(_tmp_account_page).split("\\?")[0]);
		map.put(Constants.CLICK_MARK, _tmp_click_mark);
		map.put(Constants.IS_ENTRY, _tmp_enter);
		map.put(Constants.IS_NEW, _tmp_new);
		map.put(Constants.ADSREF, replaceRegx(_tmp_adsref).split("\\?")[0]);
		map.put(Constants.ADSNEW, _tmp_adsnew);
		map.put(Constants.ADSID, _tmp_adsid);
		map.put(Constants.HOT, _tmp_hot);
		map.put(Constants.WB, _tmp_wb);
		map.put(Constants.SE, _tmp_se);
		map.put(Constants.KW, _tmp_kw);
		return map;
	}

	/**
	 * 正则匹配替换url
	 * @param text    需要匹配正则的url
	 * @return
	 */
	private static String replaceRegx(String text) {
		for (String pattern : configMap.get(Constants.PATTERN)) {
			replaceArr = pattern.split(Constants.LOG_DELIMITER);

			replaceRegex = replaceArr[0];
			replacement = replaceArr[1];
			replacePattern = Pattern.compile(replaceRegex);
			Matcher m = replacePattern.matcher(text);
			if (m.find()) {
				text = m.replaceAll(replacement);
				break;
			}
		}
		return text;
	}
}
