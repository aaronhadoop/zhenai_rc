package com.zhenai.rc.domain;

/*
 * 封装页面配置的任务属性
 * rc_task_conf
 */
public class RCTask {

	private Integer fkId;
	private String taskId;
	
	private Integer dataType; // 1 前置条件 2时间 3 维度 4 指标 
	private String colName; // 列名
	
	// 前置条件
	private String preCondition;
	
	// 时间维度
	private Integer timeInerval;
	
	// 维度
	private Integer isCollapsed;
	private String collapsedRule;
	
	// 指标
	private Integer indicatorOper; // 1 COUNT 2 COUNT DISTINCT 3 SUM 4 AVG 5 MAX 6 MIN
	private Integer indicatorOperType; // 0 增量	1 全量
	private String resColName;

	public Integer getFkId() { 
		return fkId;  
	}

	public void setFkId(Integer fkId) {
		this.fkId = fkId;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

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

	public Integer getTimeInerval() {
		return timeInerval;
	}

	public void setTimeInerval(Integer timeInerval) {
		this.timeInerval = timeInerval;
	}

	public Integer getIsCollapsed() {
		return isCollapsed;
	}

	public void setIsCollapsed(Integer isCollapsed) {
		this.isCollapsed = isCollapsed;
	}

	public String getCollapsedRule() {
		return collapsedRule;
	}

	public void setCollapsedRule(String collapsedRule) {
		this.collapsedRule = collapsedRule;
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

	public String getResColName() {
		return resColName;
	}

	public void setResColName(String resColName) {
		this.resColName = resColName;
	}

	@Override
	public String toString() {
		return "RCTask [fkId=" + fkId + ", taskId=" + taskId + ", dataType="
				+ dataType + ", colName=" + colName + ", preCondition="
				+ preCondition + ", timeInerval=" + timeInerval
				+ ", isCollapsed=" + isCollapsed + ", collapsedRule="
				+ collapsedRule + ", indicatorOper=" + indicatorOper
				+ ", indicatorOperType=" + indicatorOperType + ", resColName="
				+ resColName + "]";
	}

	public RCTask(Integer fkId, String taskId, Integer dataType,
			String colName, String preCondition, Integer timeInerval,
			Integer isCollapsed, String collapsedRule, Integer indicatorOper,
			Integer indicatorOperType, String resColName) {
		super();
		this.fkId = fkId;
		this.taskId = taskId;
		this.dataType = dataType;
		this.colName = colName;
		this.preCondition = preCondition;
		this.timeInerval = timeInerval;
		this.isCollapsed = isCollapsed;
		this.collapsedRule = collapsedRule;
		this.indicatorOper = indicatorOper;
		this.indicatorOperType = indicatorOperType;
		this.resColName = resColName;
	}

}
