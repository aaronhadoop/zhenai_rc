package com.zhenai.rc.domain;

public class RCTaskVO3 {

	private String collapsedRule;
	private String colName;
	private Integer dataType;
	private Integer isCollapsed;

	public RCTaskVO3(Integer dataType, String colName, Integer isCollapsed,
			String collapsedRule) {
		super();
		this.dataType = dataType;
		this.colName = colName;
		this.isCollapsed = isCollapsed;
		this.collapsedRule = collapsedRule;
	}
 
	public String getCollapsedRule() {
		return collapsedRule;
	}

	public String getColName() {
		return colName;
	}

	public Integer getDataType() {
		return dataType;
	}

	public Integer getIsCollapsed() {
		return isCollapsed;
	}

	public void setCollapsedRule(String collapsedRule) {
		this.collapsedRule = collapsedRule;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public void setDataType(Integer dataType) {
		this.dataType = dataType;
	}

	public void setIsCollapsed(Integer isCollapsed) {
		this.isCollapsed = isCollapsed;
	}

	@Override
	public String toString() {
//		return "RCTaskVO3 [dataType=" + dataType + ", colName=" + colName
//				+ ", isCollapsed=" + isCollapsed + ", collapsedRule="
//				+ collapsedRule + "]"; 
		return dataType + "-" + colName + "-" + isCollapsed + "-" + collapsedRule; 
	}

}