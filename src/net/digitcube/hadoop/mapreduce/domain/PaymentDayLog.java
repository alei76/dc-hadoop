package net.digitcube.hadoop.mapreduce.domain;

import net.digitcube.hadoop.util.StringUtil;

/**
 * 去重后的付费记录，按天计算
 * 
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月17日 下午7:26:38 @copyrigt www.digitcube.net <br>
 * <br>
 *          appid,platform,accountid,channel,accounttype,gender,age,gameserver,
 *          resolution ,opersystem,brand,nettype,country,province,operators
 *          ,CurrencyAmount(当日总付费),TotalPayTimes(当日付费次数)
 */

public class PaymentDayLog implements MapReduceVO {
	private String appID;
	private String platform;
	private String accountID;
	private CommonExtend extend;

	private float currencyAmount;
	private int totalPayTimes;

	//玩家当天前三次付费记录，包括付费时间以及该次付费时级别，如：
	//{payTime1:level_1,loginTime2:level_2,payTime3:level_3}
	private String payRecords = "";
	
	public final static int INDEX_AppID = 0;
	public final static int INDEX_Platform = 1;
	public final static int INDEX_AccountID = 2;
	public final static int INDEX_CurrencyAmount = 15;
	public final static int INDEX_TotalPayTimes = 16;

	public final static int INDEX_payRecords = INDEX_TotalPayTimes+1;
	
	public PaymentDayLog(String[] args) {
		appID = args[INDEX_AppID];
		platform = args[INDEX_Platform];
		accountID = args[INDEX_AccountID];
		extend = new CommonExtend(args, -2);// 向前偏移两位
		currencyAmount = StringUtil.convertFloat(args[INDEX_CurrencyAmount], 0);
		totalPayTimes = StringUtil.convertInt(args[INDEX_TotalPayTimes], 0);
		
		//payRecords 是新增属性，不同地方构造 payRecords 时可能不存在，这里兼容
		if((INDEX_payRecords + 1) == args.length ){
			payRecords = args[INDEX_payRecords];
		}
	}

	public PaymentDayLog() {
	}

	@Override
	public String[] toStringArray() {
		String[] extendArray = extend.toStringArray();
		//String[] array = new String[extendArray.length + 5];
		String[] array = new String[extendArray.length + 6];
		array[INDEX_AppID] = appID;
		array[INDEX_Platform] = platform;
		array[INDEX_AccountID] = accountID;
		System.arraycopy(extendArray, 0, array, 3, extendArray.length);
		array[INDEX_CurrencyAmount] = currencyAmount + "";
		array[INDEX_TotalPayTimes] = totalPayTimes + "";
		
		//玩家付费记录，包括付费时间及付费级别
		array[INDEX_payRecords] = payRecords;
		
		return array;
	}

	public String getAppID() {
		return appID;
	}

	public void setAppID(String appid) {
		this.appID = appid;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getAccountID() {
		return accountID;
	}

	public void setAccountID(String accountID) {
		this.accountID = accountID;
	}

	public CommonExtend getExtend() {
		return extend;
	}

	public void setExtend(CommonExtend extend) {
		this.extend = extend;
	}

	public float getCurrencyAmount() {
		return currencyAmount;
	}

	public void setCurrencyAmount(float currencyAmount) {
		this.currencyAmount = currencyAmount;
	}

	public int getTotalPayTimes() {
		return totalPayTimes;
	}

	public void setTotalPayTimes(int totalPayTimes) {
		this.totalPayTimes = totalPayTimes;
	}

	public String getPayRecords() {
		return payRecords;
	}

	public void setPayRecords(String payRecords) {
		this.payRecords = payRecords;
	};

}
