package net.digitcube.hadoop.mapreduce.hbase;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class PlayerBasicInfo {

	public static final String PLAYER_BASIC_INFO = "player_basic_info";
	public static final byte[] info = Bytes.toBytes("info");
	
	public static final byte[] level = Bytes.toBytes("level");//level
	
	public static final byte[] fstLgDay = Bytes.toBytes("fstLgDay");//firstLoginDay
	public static final byte[] lstLgDay = Bytes.toBytes("lstLgDay");//lastLoginDay
	public static final byte[] totOlDay = Bytes.toBytes("totOlDay");//totalOnlineDays
	public static final byte[] totOlTim = Bytes.toBytes("totOlTim");//totalOnlineTime
	public static final byte[] totLgTms = Bytes.toBytes("totLgTms");//totalLoginTimes
	
	public static final byte[] fstPayDay = Bytes.toBytes("fstPayDay");//firstPayDay
	public static final byte[] lstPayDay = Bytes.toBytes("lstPayDay");//lastPayDay
	public static final byte[] totPayAmt = Bytes.toBytes("totPayAmt");//totalPayAmount
	public static final byte[] totPayTms = Bytes.toBytes("totPayTms");//totalPayTimes
	
	//表名
	public static final byte[] TB_PLAYER_BASIC_INFO = Bytes.toBytes(PLAYER_BASIC_INFO);
	
	private int playerLevel = 0;
	
	private int firstLoginDay = 0;
	private int lastLoginDay = 0;
	private int totalOnlineDays = 0;
	private int totalOnlineTime = 0;
	private int totalLoginTimes = 0;
	
	private int firstPayDay = 0;
	private int lastPayDay = 0;
	private int totalPayAmount = 0;
	private int totalPayTimes = 0;
	
	public int getPlayerLevel() {
		return playerLevel;
	}
	public void setPlayerLevel(int playerLevel) {
		this.playerLevel = playerLevel;
	}
	public int getFirstLoginDay() {
		return firstLoginDay;
	}
	public void setFirstLoginDay(int firstLoginDay) {
		this.firstLoginDay = firstLoginDay;
	}
	public int getLastLoginDay() {
		return lastLoginDay;
	}
	public void setLastLoginDay(int lastLoginDay) {
		this.lastLoginDay = lastLoginDay;
	}
	public int getTotalOnlineDays() {
		return totalOnlineDays;
	}
	public void setTotalOnlineDays(int totalOnlineDays) {
		this.totalOnlineDays = totalOnlineDays;
	}
	public int getTotalOnlineTime() {
		return totalOnlineTime;
	}
	public void setTotalOnlineTime(int totalOnlineTime) {
		this.totalOnlineTime = totalOnlineTime;
	}
	public int getTotalLoginTimes() {
		return totalLoginTimes;
	}
	public void setTotalLoginTimes(int totalLoginTimes) {
		this.totalLoginTimes = totalLoginTimes;
	}
	public int getFirstPayDay() {
		return firstPayDay;
	}
	public void setFirstPayDay(int firstPayDay) {
		this.firstPayDay = firstPayDay;
	}
	public int getLastPayDay() {
		return lastPayDay;
	}
	public void setLastPayDay(int lastPayDay) {
		this.lastPayDay = lastPayDay;
	}
	public int getTotalPayAmount() {
		return totalPayAmount;
	}
	public void setTotalPayAmount(int totalPayAmount) {
		this.totalPayAmount = totalPayAmount;
	}
	public int getTotalPayTimes() {
		return totalPayTimes;
	}
	public void setTotalPayTimes(int totalPayTimes) {
		this.totalPayTimes = totalPayTimes;
	}
	
	public PlayerBasicInfo(Result result){
		if(null == result){
			return;
		}
		NavigableMap<byte[], byte[]> infoMap = result.getFamilyMap(info);
		if(null == infoMap){
			return;
		}
		
		//playerLevel
		if(null != infoMap.get(level)){
			this.playerLevel = Bytes.toInt(infoMap.get(level));
		}
		
		//firstLoginDay
		if(null != infoMap.get(fstLgDay)){
			this.firstLoginDay = Bytes.toInt(infoMap.get(fstLgDay));
		}
		
		//lastLoginDay
		if(null != infoMap.get(lstLgDay)){
			this.lastLoginDay = Bytes.toInt(infoMap.get(lstLgDay));
		}
		
		//totalOnlineDays
		if(null != infoMap.get(totOlDay)){
			this.totalOnlineDays = Bytes.toInt(infoMap.get(totOlDay));
		}
		
		//totalOnlineTime
		if(null != infoMap.get(totOlTim)){
			this.totalOnlineTime = Bytes.toInt(infoMap.get(totOlTim));
		}
		
		//totalOnlineTimes
		if(null != infoMap.get(totLgTms)){
			this.totalLoginTimes = Bytes.toInt(infoMap.get(totLgTms));
		}
		
		//firstPayDay
		if(null != infoMap.get(fstPayDay)){
			this.firstPayDay = Bytes.toInt(infoMap.get(fstPayDay));
		}
		
		//lastPayDay
		if(null != infoMap.get(lstPayDay)){
			this.lastPayDay = Bytes.toInt(infoMap.get(lstPayDay));
		}
		
		//totalPayAmount
		if(null != infoMap.get(totPayAmt)){
			this.totalPayAmount = Bytes.toInt(infoMap.get(totPayAmt));
		}
		
		//totalPayTimes
		if(null != infoMap.get(totPayTms)){
			this.totalPayTimes = Bytes.toInt(infoMap.get(totPayTms));
		}
	}
}
