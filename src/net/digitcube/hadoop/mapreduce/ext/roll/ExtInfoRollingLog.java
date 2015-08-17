
package net.digitcube.hadoop.mapreduce.ext.roll;

import java.util.HashMap;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import net.digitcube.hadoop.util.StringUtil;

public final class ExtInfoRollingLog {

	//空  map 的 json 字符 串 '{}' 用 gzip 压缩后再 base64 的字符串
	private static final String emptyMap = "H4sIAAAAAAAAAKuuBQBDv6ajAgAAAA..";
	
    private String appId;
    private String appVersion;
    private String platform;
    private String channel;
    private String gameServer;
    private String accountId;
    private String uid;
    //itemInfo 保存 itemInfoMap的json字符串 gzip压缩后的 base64值
    private String itemInfo = emptyMap;
    //guanKaInfo 保存 guanKaInfoMap的json字符串 gzip压缩后的 base64值
    private String guanKaInfo = emptyMap;
    //taskInfo 保存 taskInfoMap的json字符串 gzip压缩后的 base64值
    private String taskInfo =emptyMap;

    private Map<Integer,Map<String, ItemInfo>> itemInfoMap = null;
    private Map<Integer,Map<String, GuanKaInfo>> guanKaInfoMap = null;
    private Map<Integer,Map<String, TaskInfo>> taskInfoMap = null;

	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public String getAppVersion() {
		return appVersion;
	}
	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
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
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getItemInfo() {
		return itemInfo;
	}
	public void setItemInfo(String itemInfo) {
		this.itemInfo = itemInfo;
	}
	public String getGuanKaInfo() {
		return guanKaInfo;
	}
	public void setGuanKaInfo(String guanKaInfo) {
		this.guanKaInfo = guanKaInfo;
	}
	public String getTaskInfo() {
		return taskInfo;
	}
	public void setTaskInfo(String taskInfo) {
		this.taskInfo = taskInfo;
	}
	public Map<Integer, Map<String, ItemInfo>> getItemInfoMap() {
		if(null == itemInfoMap || itemInfoMap.isEmpty()){
			TypeToken<Map<Integer,Map<String, ItemInfo>>> type = new TypeToken<Map<Integer,Map<String, ItemInfo>>>(){};
			itemInfo = StringUtil.getStrFromBase64(itemInfo);
			itemInfoMap = StringUtil.getMapFromJson(itemInfo, type);
			if(null == itemInfoMap){
				itemInfoMap = new HashMap<Integer,Map<String, ItemInfo>>();
			}
		}
		return itemInfoMap;
	}
	public void setItemInfoMap(Map<Integer, Map<String, ItemInfo>> itemInfoMap) {
		this.itemInfoMap = itemInfoMap;
	}
	public Map<Integer, Map<String, GuanKaInfo>> getGuanKaInfoMap() {
		if(null == guanKaInfoMap || guanKaInfoMap.isEmpty()){
			TypeToken<Map<Integer,Map<String, GuanKaInfo>>> type = new TypeToken<Map<Integer,Map<String, GuanKaInfo>>>(){};
			guanKaInfo = StringUtil.getStrFromBase64(guanKaInfo);
			guanKaInfoMap = StringUtil.getMapFromJson(guanKaInfo, type);
			if(null == guanKaInfoMap){
				guanKaInfoMap = new HashMap<Integer,Map<String, GuanKaInfo>>();
			}
		}
		return guanKaInfoMap;
	}
	public void setGuanKaInfoMap(Map<Integer, Map<String, GuanKaInfo>> guanKaInfoMap) {
		this.guanKaInfoMap = guanKaInfoMap;
	}
	public Map<Integer, Map<String, TaskInfo>> getTaskInfoMap() {
		if(null == taskInfoMap || taskInfoMap.isEmpty()){
			TypeToken<Map<Integer,Map<String, TaskInfo>>> type = new TypeToken<Map<Integer,Map<String, TaskInfo>>>(){};
			taskInfo = StringUtil.getStrFromBase64(taskInfo);
			taskInfoMap = StringUtil.getMapFromJson(taskInfo, type);
			if(null == taskInfoMap){
				taskInfoMap = new HashMap<Integer,Map<String, TaskInfo>>();
			}
		}
		return taskInfoMap;
	}
	public void setTaskInfoMap(Map<Integer, Map<String, TaskInfo>> taskInfoMap) {
		this.taskInfoMap = taskInfoMap;
	}
	
	public ExtInfoRollingLog(){};
	public ExtInfoRollingLog(String[] fields){
		int i = 0;
		this.appId = fields[i++];
		this.appVersion = fields[i++];
		this.platform = fields[i++];
		this.channel = fields[i++];
		this.gameServer = fields[i++];
		this.accountId = fields[i++];
		this.uid = fields[i++];
		this.itemInfo = fields[i++];
		this.guanKaInfo = fields[i++];
		this.taskInfo = fields[i++];
    }
    
    public String[] toStringArray(){
    	//道具
    	if(null != itemInfoMap && itemInfoMap.size() > 0){
    		itemInfo = StringUtil.getJsonStr(itemInfoMap);
    		itemInfo = StringUtil.getBase64Str(itemInfo);
    	}
    	
    	//关卡
    	if(null != guanKaInfoMap && guanKaInfoMap.size() > 0){
    		guanKaInfo = StringUtil.getJsonStr(guanKaInfoMap);
    		guanKaInfo = StringUtil.getBase64Str(guanKaInfo);
    	}
    	
    	//任务
    	if(null != taskInfoMap && taskInfoMap.size() > 0){
    		taskInfo = StringUtil.getJsonStr(taskInfoMap);
    		taskInfo = StringUtil.getBase64Str(taskInfo);
    	}
    	
    	return new String[]{
    			appId, 
    			appVersion,
    			platform, 
    			channel, 
    			gameServer, 
    			accountId,
    			uid,
    			itemInfo,
    			guanKaInfo,
    			taskInfo
    	};
    }
    
    public static void main(String[] args){
    	//String s = "00007E2ECD0561179A60BACA19E0AC86	1.0	2	ID001	_ALL_GS	007E8FEDF054526125200061874CBF2F	007E8FEDF054526125200061874CBF2F	H4sIAAAAAAAAAKtWMjQxsjQ3tDAxMFCyqlYyMDAEUZklqbmeKUpWYL4OmBtSWZAKFHi6f_XLhvnP5i991rkPKJNUWumcX5pXomRlrKOUnloC5RjoKJUWp8I4tTpAc4zQzDVCNfdle--zaRueTd3yfEIbSeYao5lrjGru86YdT3Y0PF2_E8VQQzyG1tYCAHrGJoYVAQAA	H4sIAAAAAAAAAKtWMjQxsjQ3tDAxMFCyqlZ6vmaNkeXT1s0gdnppYp53omeKkhVCWEcpKTU9My8kMze1WMnKWEepuDQ5ObW4GCpgqKOUlpiZU1qUChUwqtUBaTY2wmomRJhMM40ssLvTgggzDdDNNIa60wC7Ow0wzTQk7FBDmO8NsZtqiMVUgoYaQM00xm6mMaaZFgR9b15bWwsAF0QnIwoCAAA.	{}";
    	//String s = "1901A265B0A4B41B23C9E562255AA287	1.0.1	2	slnrz_jc_eg	_ALL_GS	fb0f58d651b490f3234ca283111d3fd3	fb0f58d651b490f3234ca283111d3fd3	H4sIAAAAAAAAAKuuBQBDv6ajAgAAAA..	H4sIAAAAAAAAAKtWMjQxsjQxtTQyMFCyqlYyAhHppYl53omeKUpWQL6OUlJqemZeSGZuarGSlaGOUnFpcnJqcTFUwEBHKS0xM6e0KBWmolZHyRDNFENCphiim2JQCzIG6DRTE1MzMp2GzdBaAFOZo-fxAAAA	H4sIAAAAAAAAAKuuBQBDv6ajAgAAAA..";
    	String s = "1901A265B0A4B41B23C9E562255AA287	1.0.1.150207	2	nsgvz_pt_amazon	_ALL_GS	f7fce9cdadce8812c53e87bb4ce620e8	f7fce9cdadce8812c53e87bb4ce620e8	H4sIAAAAAAAAAKtWMjQxsjQxtTQyMFCyqlZ6vnvu0_U7X87Y9HTfQhA_syQ11zNFyQpVQgcsHlJZkAqUebq_99mc9UCxpNJK5_zSvBIlK0MdpfTUEijHQEeptDgVxqmtrQUAO0VWKnQAAAA.	H4sIAAAAAAAAAK3SOw7CMBAE0LtsncJe2_ndANFygRBMFAlSYKWKfHeyCAkxRspHNC6meNpdz0TacmVdxUpRPZFx8nZjMxybw4VqCTI6-64fTv3dB6p1RmFsWx_CJ7g2_W18-HegYkYmRycHhxedQhyDjtk8jxHHomM3O1qcAp0CHIuOShxZjCuA5uAbMgjxz8UUDqSWnHSx16UZId738xodvcOZISmmsy5fW8ykUMm9_lVMXlWElU6JTrl0rqRQHGN8Av687PjJAwAA	H4sIAAAAAAAAAKuuBQBDv6ajAgAAAA..";
    	ExtInfoRollingLog log = new ExtInfoRollingLog(s.split("\t"));
    	
    	System.out.println(log.getItemInfoMap());
    	System.out.println(log.getGuanKaInfoMap());
    	System.out.println(log.getTaskInfoMap());
    	/*System.out.println(StringUtil.getBase64Str("{}"));
    	System.out.println(StringUtil.getStrFromBase64("H4sIAAAAAAAAAKuuBQBDv6ajAgAAAA.."));*/
    }
}

