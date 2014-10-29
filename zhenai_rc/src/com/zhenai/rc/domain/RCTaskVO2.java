package com.zhenai.rc.domain;

/*
 * 作为参数传递给Bolt
 * 2--时间
 */
public class RCTaskVO2 {

	private Integer dataType = 2;
	private String colName;
	private Integer timeInerval;

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

	public Integer getTimeInerval() {
		return timeInerval;
	}

	public void setTimeInerval(Integer timeInerval) {
		this.timeInerval = timeInerval;
	}

	@Override
	public String toString() {
		return dataType + "-" + colName + "-" + timeInerval; 
	}

	public RCTaskVO2(Integer dataType, String colName, Integer timeInerval) {
		super();
		this.dataType = dataType;
		this.colName = colName;
		this.timeInerval = timeInerval;
	}

}
