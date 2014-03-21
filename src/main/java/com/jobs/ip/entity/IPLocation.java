package com.jobs.ip.entity;

import org.apache.commons.lang.StringUtils;

import com.jobs.kpi.constants.Constants;
/**
 * 
 * @author xiaorui.lu
 *
 */
public class IPLocation {
	private String country;
	private String area;
	private String province;
	private String city;
	private String district;

	public IPLocation() {
		country = area = "";
	}

	public IPLocation getCopy() {
		IPLocation ret = new IPLocation();
		ret.country = country;
		ret.area = area;
		return ret;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		// 如果为局域网，纯真IP地址库的地区会显示CZ88.NET,这里把它去掉
		if (area.trim().equals("CZ88.NET")) {
//			this.area = "本机或本网络";
			this.area = "";
		} else {
			this.area = area;
		}
	}

	public String getProvince() {
		if(this.country.trim().matches("^(北京|天津|上海|重庆)市.*"))
			this.province = this.country.substring(0, 2);
		else if(this.country.trim().matches(".+省.*"))
			this.province = this.country.substring(0, this.country.indexOf("省"));
		else if(this.country.trim().matches("^(西藏|新疆|香港|澳门).*"))
			this.province = this.country.substring(0, 2);
		else if(this.country.trim().matches("^(内蒙古).*"))
			this.province = this.country.substring(0, 3);
		else
			this.province = (StringUtils.isBlank(this.country) ? "未知" : this.country);
		return province;
	}

	public void setProvince(String province) {
			this.province = province;
	}

	public String getCity() {
		if(this.country.trim().matches("^(北京|天津|上海|重庆)市.*"))
			this.city = this.country.substring(3);
		else if(this.country.trim().matches(".+省.{2,}州.{2,}市.*"))
			this.city = this.country.substring(this.country.indexOf("州") + 1, this.country.indexOf("市")+1);
		else if(this.country.trim().matches(".+省.{2,}州.*"))
			this.city = this.country.substring(this.country.indexOf("省") + 1, this.country.indexOf("州")+1);
		else if(this.country.trim().matches(".+省.{2,}市.*"))
			this.city = this.country.substring(this.country.indexOf("省") + 1, this.country.indexOf("市")+1);
		else if(this.country.trim().matches("^(西藏|新疆|香港|澳门).+"))
			this.city = this.country.substring(2);
		else if(this.country.trim().matches("^(内蒙古).+"))
			this.city = this.country.substring(3);
		else
			this.city = "";
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getDistrict() {
		if(this.country.trim().matches("^(北京|天津|上海|重庆)市.*"))
			this.district = "";
		else if(this.country.trim().matches(".+省.{2,}州.{2,}市.*"))
			this.district = this.country.substring(this.country.indexOf("市") + 1);
		else if(this.country.trim().matches(".+省.{2,}州.*"))
			this.district = this.country.substring(this.country.indexOf("州") + 1);
		else if(this.country.trim().matches(".+省.{2,}市.*"))
			this.district = this.country.substring(this.country.indexOf("市") + 1);
		else
			this.district = "";

		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}
	
	public String getIPInfo(){
		return concatStr(getProvince(), getCity(), getArea());
	}
	
	public String concatStr(String... args) {
		StringBuilder sb = new StringBuilder();
		for (String arg : args) {
			sb.append(arg).append(Constants.MARK);
		}
		return sb.substring(0, sb.length() - 1);
	}
	
}