package net.digitcube.hadoop.mapreduce.ext.roll;

public class GuanKaInfo {

	private String guanKaId;
	private int beginTimes;
	private int successTimes;
	private int failureTimes;
	
	public String getGuanKaId() {
		return guanKaId;
	}
	public void setGuanKaId(String guanKaId) {
		this.guanKaId = guanKaId;
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
		return "GuanKaInfo [guanKaId=" + guanKaId + ", beginTimes="
				+ beginTimes + ", successTimes=" + successTimes
				+ ", failureTimes=" + failureTimes + "]";
	}
}
