package net.digitcube.hadoop.mapreduce.adtrack;

public class CompareAttr {

	private String type;
	private String ip;
	private int timestamp;
	private String province;
	private String operator;

	private String channel;
	private String uid;

	public CompareAttr(String[] paramArray) {
		type = paramArray[0];
		timestamp = Integer.parseInt(paramArray[1]);
		ip = paramArray[2];
		province = paramArray[3];
		operator = paramArray[4];

		if (isFromClick()) {
			channel = paramArray[5];
		} else {
			uid = paramArray[5];
		}
	}

	public boolean isFromClick() {
		return "C".equals(type);
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

}
