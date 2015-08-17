package net.digitcube.hadoop.mapreduce.ext.roll;

public class TaskInfo {

	private String taskId;
	private String taskType;
	private int beginTimes;
	private int successTimes;
	private int failureTimes;
	
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	public String getTaskType() {
		return taskType;
	}
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	public int getBeginTimes() {
		return beginTimes;
	}
	public void setBeginTimes(int beginTimes) {
		this.beginTimes = beginTimes;
	}
	public int getSuccessTimes() {
		return successTimes;
	}
	public void setSuccessTimes(int successTimes) {
		this.successTimes = successTimes;
	}
	public int getFailureTimes() {
		return failureTimes;
	}
	public void setFailureTimes(int failureTimes) {
		this.failureTimes = failureTimes;
	}
	
	@Override
	public String toString() {
		return "TaskInfo [taskId=" + taskId + ", taskType=" + taskType
				+ ", beginTimes=" + beginTimes + ", successTimes="
				+ successTimes + ", failureTimes=" + failureTimes + "]";
	}
	
}
