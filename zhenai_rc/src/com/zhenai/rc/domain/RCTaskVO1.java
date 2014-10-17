package com.zhenai.rc.domain;

/*
 * 作为参数传递给Bolt
 * 1--前置条件
 */
public class RCTaskVO1 implements java.io.Serializable{
	private static final long serialVersionUID = 1L;
	
	private Integer dataType;
	private String colName;
	private String preCondition;

	public Integer getDataType() {
		return dataType;
	}

	public void setDataType(Integer dataType) {
		this.dataType = dataType;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getPreCondition() {
		return preCondition;
	}

	public void setPreCondition(String preCondition) {
		this.preCondition = preCondition;
	}

	@Override
	public String toString() {
//		return "RCTaskVO1 [dataType=" + dataType + ", colName=" + colName
//				+ ", preCondition=" + preCondition + "]";
//		return dataType + "." + colName + "." + preCondition; 
//		return "192.168.131.134, 192.168.131.141, 192.168.131.142"; 
		return "11111"; 
	}

	public RCTaskVO1(Integer dataType, String colName, String preCondition) {
		super();
		this.dataType = dataType;
		this.colName = colName;
		this.preCondition = preCondition;
	}

}
