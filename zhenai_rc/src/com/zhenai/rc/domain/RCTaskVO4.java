package com.zhenai.rc.domain;

/*
 * 作为参数传递给Bolt
 * 4--指标
 */
public class RCTaskVO4 {

	private Integer dataType;
	private String colName;
	private Integer indicatorOper;
	private Integer indicatorOperType;
 
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

	public Integer getIndicatorOper() {
		return indicatorOper;
	}

	public void setIndicatorOper(Integer indicatorOper) {
		this.indicatorOper = indicatorOper;
	}

	public Integer getIndicatorOperType() {
		return indicatorOperType;
	}

	public void setIndicatorOperType(Integer indicatorOperType) {
		this.indicatorOperType = indicatorOperType;
	}

	public RCTaskVO4(Integer dataType, String colName, Integer indicatorOper,
			Integer indicatorOperType) {
		super();
		this.dataType = dataType;
		this.colName = colName;
		this.indicatorOper = indicatorOper;
		this.indicatorOperType = indicatorOperType;
	}

	@Override
	public String toString() {
//		return "RCTaskVO4 [dataType=" + dataType + ", colName=" + colName
//				+ ", indicatorOper=" + indicatorOper + ", indicatorOperType="
//				+ indicatorOperType + "]";
		return dataType + "-" + colName + "-" + indicatorOper + "-" + indicatorOperType; 
	}

	
}