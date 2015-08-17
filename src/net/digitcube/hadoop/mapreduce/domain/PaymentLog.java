package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.util.StringUtil;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 下午7:59:44 @copyrigt www.digitcube.net
 */

public class PaymentLog implements MapReduceVO {

	private float currencyAmount;
	private float virtualCurrencyAmount;
	private String iapid;
	private String currencyType;
	private String payType;
	private int level;
	private int payTime;

	public final static int INDEX_CurrencyAmount = 17;
	public final static int INDEX_VirtualCurrencyAmount = 18;
	public final static int INDEX_Iapid = 19;
	public final static int INDEX_CurrencyType = 20;
	public final static int INDEX_PayType = 21;
	public final static int INDEX_Level = 22;
	public final static int INDEX_PayTime = 23;

	public PaymentLog() {
	}

	public PaymentLog(String[] args) {
		currencyAmount = StringUtil.convertFloat(args[INDEX_CurrencyAmount], 0);
		virtualCurrencyAmount = StringUtil.convertFloat(
				args[INDEX_VirtualCurrencyAmount], 0);
		iapid = args[INDEX_Iapid];
		currencyType = args[INDEX_CurrencyType];
		payType = args[INDEX_PayType];
		level = StringUtil.convertInt(args[INDEX_Level], 0);
		payTime = StringUtil.convertInt(args[INDEX_PayTime], 0);
	}

	@Override
	public String[] toStringArray() {
		return new String[] { currencyAmount + "", virtualCurrencyAmount + "",
				iapid, currencyType, payType, level + "", payTime + "" };
	}

	public float getCurrencyAmount() {
		return currencyAmount;
	}

	public void setCurrencyAmount(float currencyAmount) {
		this.currencyAmount = currencyAmount;
	}

	public float getVirtualCurrencyAmount() {
		return virtualCurrencyAmount;
	}

	public void setVirtualCurrencyAmount(float virtualCurrencyAmount) {
		this.virtualCurrencyAmount = virtualCurrencyAmount;
	}

	public String getIapid() {
		return iapid;
	}

	public void setIapid(String iapid) {
		this.iapid = iapid;
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

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public int getPayTime() {
		return payTime;
	}

	public void setPayTime(int payTime) {
		this.payTime = payTime;
	}

}
