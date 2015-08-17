package net.digitcube.hadoop.model;

import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.util.StringUtil;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class PaymentLog extends HeaderLog {
	static final String NaN = "NaN";
	
	private float currencyAmount;
	private float virtualCurrencyAmount;
	private String iapid;
	private String currencyType;
	private String payType;
	private int level;
	private int payTime;
	private String extInfo; // 20141204 : 付费增加扩展字段

	private Map<String, String> payMap;

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

	public String getExtInfo() {
		return extInfo;
	}

	public void setExtInfo(String extInfo) {
		this.extInfo = extInfo;
	}

	public PaymentLog(String[] fields) {
		super(fields);
		int length = fields.length;
		// 增加 extInfo 后长度为 31
		if (length > 30) {
			this.currencyAmount = StringUtil
					.convertFloat(fields[length - 8], 0);
			checkCurrency(currencyAmount);
			this.virtualCurrencyAmount = StringUtil.convertFloat(
					fields[length - 7], 0);
			this.iapid = fields[length - 6];
			this.currencyType = fields[length - 5];
			this.payType = fields[length - 4];
			this.level = StringUtil.convertInt(fields[length - 3], 0);
			this.payTime = StringUtil.convertInt(fields[length - 2], 0);
			this.extInfo = fields[length - 1];

			setAttrMap(this.extInfo);
		} else {
			this.currencyAmount = StringUtil
					.convertFloat(fields[length - 7], 0);
			checkCurrency(currencyAmount);
			this.virtualCurrencyAmount = StringUtil.convertFloat(
					fields[length - 6], 0);
			this.iapid = fields[length - 5];
			this.currencyType = fields[length - 4];
			this.payType = fields[length - 3];
			this.level = StringUtil.convertInt(fields[length - 2], 0);
			this.payTime = StringUtil.convertInt(fields[length - 1], 0);
		}
		
		if(this.currencyAmount > 100000000 || this.currencyAmount <= 0){
			throw new RuntimeException("Invalid currency amount.");
		}
	}

	public void setFields(String[] fields) {
		super.setFields(fields);
		int length = fields.length;
		// 增加 extInfo 后长度为 31
		if (length > 30) {
			this.currencyAmount = StringUtil
					.convertFloat(fields[length - 8], 0);
			checkCurrency(currencyAmount);
			this.virtualCurrencyAmount = StringUtil.convertFloat(
					fields[length - 7], 0);
			this.iapid = fields[length - 6];
			this.currencyType = fields[length - 5];
			this.payType = fields[length - 4];
			this.level = StringUtil.convertInt(fields[length - 3], 0);
			this.payTime = StringUtil.convertInt(fields[length - 2], 0);
			this.extInfo = fields[length - 1];

			setAttrMap(this.extInfo);
		} else {
			this.currencyAmount = StringUtil
					.convertFloat(fields[length - 7], 0);
			checkCurrency(currencyAmount);
			this.virtualCurrencyAmount = StringUtil.convertFloat(
					fields[length - 6], 0);
			this.iapid = fields[length - 5];
			this.currencyType = fields[length - 4];
			this.payType = fields[length - 3];
			this.level = StringUtil.convertInt(fields[length - 2], 0);
			this.payTime = StringUtil.convertInt(fields[length - 1], 0);
		}
	}

	public void setAttrMap(String payAttr) {
		payMap = new HashMap<String, String>();

		// 如果事件属性值为 '-'，说明没有属性
		if (MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(payAttr)) {
			return;
		}

		try {
			Gson gson = new Gson();
			payMap = gson.fromJson(payAttr,
					new TypeToken<Map<String, String>>() {
					}.getType());
		} catch (Throwable t) {
		}
	}

	public String getGuankaId() {
		if (null == payMap) {
			return null;
		}

		return payMap.get("levelsId");
	}

	public String getOrderId() {
		if (null == payMap) {
			return null;
		}

		return payMap.get("OrderId");
	}

	private void checkCurrency(float currency){
		if(NaN.equals(""+currency)){
			throw new RuntimeException("Currency Amount is not a number.");
		}
	}
	
	public static void main(String[] args) {
		String s = "1417619629047	1	8EFA6FD1DC7E2A283EF56EF6BEADC35B|1.7	4230d1b52f688c1ee35164af3c1cb1ed	1989840794909	2	com.warhegem.iapp	1	2	0	10F风云争霸	540x960	4.1.1	三星GT-N7102	3	中国	广东省	中国联通	-	37309	-	112.96.129.142	-	590	5900	Gold	CNY	Pay	39	1417619632	-";
		String s1 = "1417619629047	1	8EFA6FD1DC7E2A283EF56EF6BEADC35B|1.7	4230d1b52f688c1ee35164af3c1cb1ed	1989840794909	2	com.warhegem.iapp	1	2	0	10F风云争霸	540x960	4.1.1	三星GT-N7102	3	中国	广东省	中国联通	-	37309	-	112.96.129.142	-	590	5900	Gold	CNY	Pay	39	1417619632";
		PaymentLog log = new PaymentLog(s.split("\t"));
		System.out.println(log.getCurrencyAmount());

		PaymentLog log1 = new PaymentLog(s1.split("\t"));
		System.out.println(log1.getCurrencyAmount());
	}
}
