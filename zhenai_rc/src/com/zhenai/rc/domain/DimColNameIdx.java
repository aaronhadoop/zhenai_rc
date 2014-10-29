package com.zhenai.rc.domain;

public class DimColNameIdx {

	private String colName;
	private Integer idx;

	public String getColName() {
		return colName;
	} 

	public void setColName(String colName) {
		this.colName = colName;
	}

	public Integer getIdx() {
		return idx;
	}

	public void setIdx(Integer idx) {
		this.idx = idx;
	}

	public DimColNameIdx(String colName, Integer idx) {
		super();
		this.colName = colName;
		this.idx = idx;
	}

	@Override
	public String toString() {
		return "ColNameIdx [colName=" + colName + ", idx=" + idx + "]";
	}

}

