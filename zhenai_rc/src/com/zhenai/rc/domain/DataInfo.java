package com.zhenai.rc.domain;

/*
 * data_info
 */
public class DataInfo {

	private String dataId;
	private String colName;
	private String comment;

	public DataInfo(String dataId, String colName, String comment) {
		super();
		this.dataId = dataId;
		this.colName = colName;
		this.comment = comment;
	}

	public String getDataId() {
		return dataId;
	}

	public void setDataId(String dataId) {
		this.dataId = dataId;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	@Override
	public String toString() {
		return "DataInfo [dataId=" + dataId + ", colName=" + colName
				+ ", comment=" + comment + "]";
	}

}
