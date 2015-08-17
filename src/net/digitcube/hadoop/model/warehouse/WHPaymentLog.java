package net.digitcube.hadoop.model.warehouse;

import net.digitcube.hadoop.util.StringUtil;

public class WHPaymentLog extends WHHeaderLog {

	private int payTime;
	private float currencyAmount;
	private String currencyType;
	private String payType;
	private String virtualCurrencyAmount;
	private String orderId;

	public void setFields(String[] attr) {
		super.setFields(attr);
		if (attr.length < 18) {
			throw new RuntimeException("Invalid WHPaymentLog, fields is less than 18...");
		}
		
		int idx = WHHeaderLog.SIZE;
		this.payTime = StringUtil.convertInt(attr[idx++], 0);
		this.currencyAmount = StringUtil.convertFloat(attr[idx++], 0);
		this.currencyType = attr[idx++];
		this.payType = attr[idx++];
		this.virtualCurrencyAmount = attr[idx++];
		this.orderId = attr[idx++];
	}

	public WHPaymentLog(String[] attr) {
		super(attr);
		setFields(attr);
	}

	public int getPayTime() {
		return payTime;
	}

	public void setPayTime(int payTime) {
		this.payTime = payTime;
	}

	public float getCurrencyAmount() {
		return currencyAmount;
	}

	public void setCurrencyAmount(float currencyAmount) {
		this.currencyAmount = currencyAmount;
	}

	public String getCurrencyType() {
		return currencyType;
	}

	public void setCurrencyType(String currencyType) {
		this.currencyType = currencyType;
	}

	public String getPayType() {
		return payType;
	}

	public void setPayType(String payType) {
		this.payType = payType;
	}

	public String getVirtualCurrencyAmount() {
		return virtualCurrencyAmount;
	}

	public void setVirtualCurrencyAmount(String virtualCurrencyAmount) {
		this.virtualCurrencyAmount = virtualCurrencyAmount;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String[] returnArray() {
		return new String[] { getUid(), getAccountId(), getAppId(), getPlatform() + "", getChannel(), getLevel() + "",
				getTs() + "", getIp(), getCountry(), getProvince(), getCity(), getCleanFlag(), getPayTime() + "",
				getCurrencyAmount() + "", getCurrencyType(), getPayType(), getVirtualCurrencyAmount() + "",
				getOrderId() };
	}
}
