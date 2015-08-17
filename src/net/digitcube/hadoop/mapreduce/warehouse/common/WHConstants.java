package net.digitcube.hadoop.mapreduce.warehouse.common;

public class WHConstants {
	
	public static final int IDX_UID = 0;
	public static final int IDX_ACCOUNTID = 1;
	public static final int IDX_APPID = 2;
	public static final int IDX_PLATFORM = 3;
	public static final int IDX_CHANNEL= 4;
	public static final int IDX_LEVEL = 5;
	public static final int IDX_TS = 6;
	public static final int IDX_IP = 7;
	public static final int IDX_COUNTRY = 8;
	public static final int IDX_PROVINCE = 9;
	public static final int IDX_CITY = 10;
	public static final int IDX_CLAENFLAG = 11;
	
	
	public static final int IDX_ONLINE_LOGINTIME = 12;
	public static final int IDX_ONLINE_DURATION = 13;
	public static final int IDX_ONLINE_BRAND = 14;
	public static final int IDX_ONLINE_RESOLUTION = 15;
	public static final int IDX_ONLINE_OS = 16;
	public static final int IDX_ONLINE_MAC = 17;
	public static final int IDX_ONLINE_IMEI = 18;
	public static final int IDX_ONLINE_IMSI = 19;
	public static final int IDX_ONLINE_IDFA = 20;
	public static final int IDX_ONLINE_OPERATOR = 21;
	public static final int IDX_ONLINE_NETTYPE = 22;
	
	public static final int IDX_ONLINE_TOTALLOGINTIMES = 12;
	public static final int IDX_ONLINE_TOTALDURATION = 13;
	public static final int IDX_ONLINE_HOURRECORD = 21;
	public static final int IDX_ONLINE_DAYOFWEEK = 22;
	
	public static final int IDX_PAYMENT_PAYTIME = 12;
	public static final int IDX_PAYMENT_CURRENCYAMOUNT = 13;
	public static final int IDX_PAYMENT_CURRENCYTYPE = 14;
	public static final int IDX_PAYMENT_PAYTYPE = 15;
	public static final int IDX_PAYMENT_VIRTUALCURRENCYAMOUNT = 16;
	public static final int IDX_PAYMENT_ORDERID = 17;
	
	public static final int IDX_PAYMENT_TOTALPAYTIMES = 12;
	public static final int IDX_PAYMENT_TOTALPAYAMOUNT = 13;
	public static final int IDX_PAYMENT_HOURRECORD = 16;
	public static final int IDX_PAYMENT_DAYOFWEEK = 17;

	
	public static final int SECONDS_IN_ONE_DAY = 3600 * 24;
	public final static int DIM_LOGIN_TIMES = 1;
	public final static int DIM_DURATION = 2;
	public final static int DIM_PAY_TIMES = 3;
	public final static int DIM_PAY_AMOUNT = 4;
	public final static int TYPE_ALL = 1;
	public final static int TYPE_WEEKDAY = 2;
	public final static int TYPE_WEEKEND = 3;
}
