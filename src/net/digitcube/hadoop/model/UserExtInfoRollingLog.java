
package net.digitcube.hadoop.model;

import net.digitcube.hadoop.jce.GZIPUtils;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ExtendInfoDetailMap;
import net.digitcube.hadoop.util.Base64Ext;
import net.digitcube.protocol.JceInputStream;

public final class UserExtInfoRollingLog {

    public String appId;
    public String platform;
    public String channel;
    public String gameServer;
    public String accountID;
    public String detailInfo;

    //保存玩家每天的道具购买、关卡及任务详情
    public ExtendInfoDetailMap detailMap = null;

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getGameServer() {
		return gameServer;
	}

	public void setGameServer(String gameServer) {
		this.gameServer = gameServer;
	}

	public String getAccountID() {
		return accountID;
	}

	public void setAccountID(String accountID) {
		this.accountID = accountID;
	}

	public String getDetailInfo() {
		return detailInfo;
	}

	public void setDetailInfo(String detailInfo) {
		this.detailInfo = detailInfo;
	}

	
	public ExtendInfoDetailMap getDetailMap() {
		if(null == detailMap){
			detailMap = decodeFromBase64Str(detailInfo);
		}
		return detailMap;
	}

	public void setDetailMap(ExtendInfoDetailMap detailMap) {
		this.detailMap = detailMap;
	}

	public UserExtInfoRollingLog(){
	}
	
	public UserExtInfoRollingLog(String[] fields)
    {
		int i = 0;
        setAppId(fields[i++]);
        setPlatform(fields[i++]);
        setChannel(fields[i++]);
        setGameServer(fields[i++]);
        setAccountID(fields[i++]);
        setDetailInfo(fields[i++]);
    }

    public UserExtInfoRollingLog(String appId, String platform, String channel, String gameServer, String accountID, String detailInfo)
    {
        setAppId(appId);
        setPlatform(platform);
        setChannel(channel);
        setGameServer(gameServer);
        setAccountID(accountID);
        setDetailInfo(detailInfo);
    }
    
    public String[] toStringArray(){
    	detailInfo = encode2Base64Str();
    	return new String[]{appId, platform, channel, gameServer, accountID, detailInfo};
    }
    
    public ExtendInfoDetailMap decodeFromBase64Str(String infoBase64){
    	try {
    		byte[] data = Base64Ext.decode(infoBase64);
    		data = GZIPUtils.decompress(data);// 解压
    		JceInputStream inputStream = new JceInputStream(data);
    		inputStream.setServerEncoding("UTF-8");
    		
    		ExtendInfoDetailMap detailMap = new ExtendInfoDetailMap();
    		detailMap.readFrom(inputStream);
    		
    		return detailMap;
    	} catch (Throwable t) {
    		t.printStackTrace();
    	}
    	return new ExtendInfoDetailMap();
    }
    
    public String encode2Base64Str(){
    	try {	
    		byte[] data = detailMap.toByteArray("UTF-8");
    		data = GZIPUtils.compress(data); // 压缩
    		return Base64Ext.encode(data);
    	} catch (Throwable t) {
    		t.printStackTrace();
    	}
    	return "";
    }
}

