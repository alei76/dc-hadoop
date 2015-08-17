package net.digitcube.hadoop.mapreduce.html5.html5new.vo;

import net.digitcube.hadoop.mapreduce.domain.MapReduceVO;
import net.digitcube.hadoop.util.StringUtil;

public class H5PvDayLog implements MapReduceVO {
	
	private String appId;
	private String accountId;
	private String platform; 
	private String h5app;
	private String h5domain;
	private String h5ref;
	private int h5crtime;
	private String uid;
	private int playerTotalPVs;
	private int player1ViewPvs;
	private String playerPVRecords;
	 	
    public H5PvDayLog(){
    	
    }
    
    public H5PvDayLog(String[] args) {
    	appId = args[0];
    	accountId = args[1];
    	platform = args[2];
    	h5app = args[3];
    	h5domain = args[4];
    	h5ref = args[5];
    	h5crtime = StringUtil.convertInt(args[6],0);
    	uid = args[7];
    	playerTotalPVs = StringUtil.convertInt(args[8],0);
    	player1ViewPvs = StringUtil.convertInt(args[9],0);    
    	playerPVRecords = args[10];
    }
    
	@Override
	public String[] toStringArray() {
		String[] array = new String[13];
		array[0] = appId;
		array[1] = accountId;
		array[2] = platform;
		array[3] = h5app;
		array[4] = h5domain;
		array[5] = h5ref;
		array[6] = h5crtime + "";
		array[7] = uid;
		array[8] = playerTotalPVs + "";
		array[9] = player1ViewPvs + "";
		array[10] =playerPVRecords;
		return array;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAccountId() {
		return accountId;
	}
	
	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}
	
	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getH5app() {
		return h5app;
	}

	public void setH5app(String h5app) {
		this.h5app = h5app;
	}

	public String getH5domain() {
		return h5domain;
	}

	public void setH5domain(String h5domain) {
		this.h5domain = h5domain;
	}

	public String getH5ref() {
		return h5ref;
	}

	public void setH5ref(String h5ref) {
		this.h5ref = h5ref;
	}

	public int getH5crtime() {
		return h5crtime;
	}

	public void setH5crtime(int h5crtime) {
		this.h5crtime = h5crtime;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public int getPlayerTotalPVs() {
		return playerTotalPVs;
	}

	public void setPlayerTotalPVs(int playerTotalPVs) {
		this.playerTotalPVs = playerTotalPVs;
	}

	public int getPlayer1ViewPvs() {
		return player1ViewPvs;
	}

	public void setPlayer1ViewPvs(int player1ViewPvs) {
		this.player1ViewPvs = player1ViewPvs;
	}

	public String getPlayerPVRecords() {
		return playerPVRecords;
	}

	public void setPlayerPVRecords(String playerPVRecords) {
		this.playerPVRecords = playerPVRecords;
	}

	
}
