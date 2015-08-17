package net.digitcube.hadoop.mapreduce.tag;

import java.util.HashMap;
import java.util.Map;
import com.google.common.reflect.TypeToken;

import net.digitcube.hadoop.jce.GZIPUtils;
import net.digitcube.hadoop.util.Base64Ext;
import net.digitcube.hadoop.util.StringUtil;

public class TagInfoLog {

	private static final TypeToken<Map<String, TagInfo>> type = new TypeToken<Map<String, TagInfo>>(){
		private static final long serialVersionUID = 8631363948490380251L;
	};
	
	private static final String UTF8 = "UTF-8";
	
	private String appId;
	private String appVer;
	private String platform;
	private String channel;
	private String gameServer;
	private String accountId;
	private int level;
	private int loginTrack;
	private int payTrack;
	private String tagsInfo = "";
	
	
	/**
	 * tagsMap : 该玩家一级标签集合
	 * key 为标签名称, value TagInfo 为标签详情
	 * 其中 TagInfo 中包含自身标签信息以及所有子标签详情
	 */
	private Map<String, TagInfo> tagsMap;
	
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public String getAppVer() {
		return appVer;
	}
	public void setAppVer(String appVer) {
		this.appVer = appVer;
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
	public String getAccountId() {
		return accountId;
	}
	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	public int getLoginTrack() {
		return loginTrack;
	}
	public void setLoginTrack(int loginTrack) {
		this.loginTrack = loginTrack;
	}
	public int getPayTrack() {
		return payTrack;
	}
	public void setPayTrack(int payTrack) {
		this.payTrack = payTrack;
	}
	public String getTagsInfo() {
		return tagsInfo;
	}
	public void setTagsInfo(String tagsInfo) {
		this.tagsInfo = tagsInfo;
	}
	public Map<String, TagInfo> getTagsMap() {
		if(null == tagsMap){
			initTagsMap();
		}
		return tagsMap;
	}
	public void setTagsMap(Map<String, TagInfo> tagsMap) {
		this.tagsMap = tagsMap;
	}

	public TagInfoLog(){}
	public TagInfoLog(String[] param){
		int i = 0;
		this.appId = param[i++];
		this.appVer = param[i++];
		this.platform = param[i++];
		this.channel = param[i++];
		this.gameServer = param[i++];
		this.accountId = param[i++];
		this.level = StringUtil.convertInt(param[i++], 0);
		this.loginTrack = StringUtil.convertInt(param[i++], 0);
		this.payTrack = StringUtil.convertInt(param[i++], 0);
		this.tagsInfo = param[i++];
	}
	
	public void markLogin(boolean isLogin) {
		loginTrack = loginTrack << 1 | (isLogin ? 1 : 0);
	}
	public void markPay(boolean isPay) {
		payTrack = payTrack << 1 | (isPay ? 1 : 0);
	}
	public boolean isLogin(int targetDate, int statDate) {
		// 与最新结算日期的间隔
		int days = (statDate - targetDate) / (24 * 3600);
		if (days < 0 || days > 31){
			// 超过记录的范围 直接返回false
			return false;
		}
			
		return ((loginTrack >> days) & 1) > 0;
	}
	public boolean isEverLogin(int everDaysTrack) {
		if (everDaysTrack > 32){
			// 超过记录的范围 直接返回false
			return false;
		}
			
		int track = loginTrack << (32 - everDaysTrack);
		return track != 0;
	}
	public boolean isPay(int targetDate, int statDate) {
		// 与最新日期的间隔
		int days = (statDate - targetDate) / (24 * 3600);
		if (days < 0 || days > 31){
			// 超过记录的范围 直接返回false
			return false;
		}
			
		return ((payTrack >> days) & 1) > 0;
	}
	
	/**
	 * 添加一级标签
	 * 注：如果标签已存在则将 isActive 状态置为 true
	 * @param tagName
	 */
	public void addTag(String tagName, int createTime){
		if(null == tagsMap){
			initTagsMap();
		}
		TagInfo tagInfo = tagsMap.get(tagName);
		if(null != tagInfo){
			tagInfo.setActive(true);
		}else{
			tagInfo = new TagInfo(tagName, createTime);
			tagsMap.put(tagName, tagInfo);
		}
	}
	/**
	 * 删除一级标签及一级标签下所有二级标签
	 * 注：删除只会将标签的 isActive 状态置为 false
	 * @param tagName
	 */
	public void removeTag(String tagName, int removeTime){
		if(null == tagsMap){
			initTagsMap();
		}
		TagInfo tagInfo = tagsMap.get(tagName);
		if(null != tagInfo && tagInfo.isActive()){
			tagInfo.rmTag(removeTime);
		}
	}
	/**
	 * 添加二级标签，如果一级标签不存在则同时添加一级标签
	 * @param tagName
	 * @param subTagName
	 * @param createTime
	 */
	public void addSubTag(String tagName, String subTagName, int createTime){
		if(null == tagsMap){
			initTagsMap();
		}
		TagInfo tagInfo = tagsMap.get(tagName);
		if(null != tagInfo){
			tagInfo.setActive(true);
		}else{
			tagInfo = new TagInfo(tagName, createTime);
			tagsMap.put(tagName, tagInfo);
		}
		
		tagInfo.addSubTag(subTagName, createTime);
	}
	/**
	 * 仅删除某个二级标签
	 * @param tagName
	 * @param subTagName
	 */
	public void removeSubTag(String tagName, String subTagName, int removeTime){
		if(null == tagsMap){
			initTagsMap();
		}
		TagInfo tagInfo = tagsMap.get(tagName);
		if(null != tagInfo){
			tagInfo.rmSubTag(subTagName, removeTime);
		}
	}
	
	synchronized private void initTagsMap(){
		if(null != tagsMap){
			return;
		}
		if(!StringUtil.isEmpty(tagsInfo)){
			String tagsJson = base64Decode(tagsInfo);
			tagsMap = StringUtil.getMapFromJson(tagsJson, type);
		}else{
			tagsMap = new HashMap<String, TagInfo>();
		}
	}
	
	public String[] toStringArray(){
		if(null != tagsMap){
			String tagsJson = StringUtil.getJsonStr(tagsMap);
			tagsInfo = base64Encode(tagsJson);
		}
		return new String[]{
				this.appId,
				this.appVer,
				this.platform,
				this.channel,
				this.gameServer,
				this.accountId,
				this.level+"",
				this.loginTrack+"",
				this.payTrack+"",
				tagsInfo
		};
	}
	
	private static String base64Encode(String jsonStr){
		try {
			byte[] data = jsonStr.getBytes(UTF8);
			data = GZIPUtils.compress(data);
			return Base64Ext.encode(data);
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	private static String base64Decode(String base64Str){
		try {
			byte[] data = Base64Ext.decode(base64Str);
			data = GZIPUtils.decompress(data);
			return new String(data, UTF8);
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 标签信息(理论上支持无限级标签)：
	 * a) 存储标签本身信息，包括：标签名称，创建时间，标签是否激活
	 * b) 该标签所有子标签信息
	 * 
	 * 注：增加标签时，标签激活标识为 true，删除标签时，标记激活标识为 false
	 */
	class TagInfo{
		private String parName;
		private String tagName;
		private int createTime;
		private int removeTime;
		private boolean isActive;
		private Map<String, TagInfo> subTagsMap = new HashMap<String, TagInfo>();
		
		public String getParName() {
			return parName;
		}
		public void setParName(String parName) {
			this.parName = parName;
		}
		public String getTagName() {
			return tagName;
		}
		public void setTagName(String tagName) {
			this.tagName = tagName;
		}
		public int getCreateTime() {
			return createTime;
		}
		public void setCreateTime(int createTime) {
			this.createTime = createTime;
		}
		public int getRemoveTime() {
			return removeTime;
		}
		public void setRemoveTime(int removeTime) {
			this.removeTime = removeTime;
		}
		public boolean isActive() {
			return isActive;
		}
		public void setActive(boolean isActive) {
			this.isActive = isActive;
		}
		public Map<String, TagInfo> getSubTagsMap() {
			return subTagsMap;
		}
		public void setSubTagsMap(Map<String, TagInfo> subTagsMap) {
			this.subTagsMap = subTagsMap;
		}
		
		public TagInfo(String tagName, int createTime){
			this(null, tagName, createTime);
		}
		
		public TagInfo(String parName, String tagName, int createTime){
			this.parName = parName;
			this.tagName = tagName;
			this.createTime = createTime;
			this.isActive = true;
		}
		
		public void rmTag(int removeTime){
			this.removeTime = removeTime;
			this.isActive = false;
			for(TagInfo subTag : subTagsMap.values()){
				subTag.rmTag(removeTime);
			}
		}
		
		public void addSubTag(String subTagName, int createTime){
			TagInfo child = subTagsMap.get(subTagName);
			if(null == child){
				child = new TagInfo(this.tagName, subTagName, createTime);
				subTagsMap.put(subTagName, child);
			}else if(!child.isActive){//重新激活标签
				child.isActive = true;
			}
		}
		
		public void rmSubTag(String subTagName, int removeTime){
			TagInfo subTag = subTagsMap.get(subTagName);
			if(null != subTag){
				subTag.rmTag(removeTime);
			}
		}
		
		@Override
		public String toString() {
			return "TagInfo [parName=" + parName + ", tagName=" + tagName
					+ ", createTime=" + createTime + ", removeTime="
					+ removeTime + ", isActive=" + isActive + ", subTagsMap="
					+ subTagsMap + "]";
		}
	}
	
	public static void main(String[] args){
		String s1 = "147258369abcdefg";
		String s2 = base64Encode(s1);
		System.out.println(s1);
		System.out.println(s2);
		System.out.println(base64Decode(s2));
	}
}
