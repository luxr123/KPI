package com.jobs.kpi.entity;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import com.jobs.kpi.constants.Constants;

/**
 * 
 * Simple to Introduction
 * @ProjectName:  [KPI]
 * @Package:      [com.jobs.kpi.entity]
 * @ClassName:    [Hot]
 * @Description:  [坐标热力图的实体类]
 * @Author:       [xiaorui.lu]
 * @CreateDate:   [2014年3月21日 下午2:13:32]
 * @UpdateUser:   [xiaorui.lu]
 * @UpdateDate:   [2014年3月21日 下午2:13:32]
 * @UpdateRemark: [说明本次修改内容]
 * @Version:      [v1.0]
 *
 */
public class Hot {

	private String x;
	private String y;
	private String t;
	private String u;

	public String getX() {
		return x;
	}

	public void setX(String x) {
		this.x = x;
	}

	public String getY() {
		return y;
	}

	public void setY(String y) {
		this.y = y;
	}

	public String getT() {
		return t;
	}

	public void setT(String t) {
		this.t = t;
	}

	public String getU() {
		return u;
	}

	public void setU(String u) {
		try {
			this.u = URLDecoder.decode(u, "UTF-8").split("\\?")[0];
		} catch (UnsupportedEncodingException e) {
			System.err.println("链接热击图中的json数据的u解码出错了...");
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		if ("a".equals(t))
			return x + Constants.MARK + y + Constants.MARK + u;
		return x + Constants.MARK + y;
	}
}
