package net.digitcube.hadoop.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.util.StringUtil;

public class EventLog extends HeaderLog {

	
	private String eventId;
	private int duration;
	private String eventAttr;
	private Map<String, String> arrtMap;
	
	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public String getEventAttr() {
		return eventAttr;
	}

	public void setEventAttr(String eventAttr) {
		this.eventAttr = eventAttr;
	}

	public Map<String, String> getArrtMap() {
		return arrtMap;
	}

	public void setArrtMap(Map<String, String> arrtMap) {
		this.arrtMap = arrtMap;
	}

	public EventLog(String[] fields) {
		super(fields);
		//Added at 20140808：事件字段数最少不能少于 20 个
		if(fields.length < 20){
			throw new RuntimeException("Invalid event, fields is less than 20...");
		}
		int length = fields.length;
		this.eventId = fields[length -3];
		this.duration = StringUtil.convertInt(fields[length -2], 0);
		this.eventAttr = fields[length -1];
		
		setAttrMap(this.eventAttr);
		
		//如果是升级事件，用 online duration 代替 duration
		resetDuraForLevelUpEvent(eventId, fields);
	}
	
	public void setFields(String[] fields){
		super.setFields(fields);
		int length = fields.length;
		this.eventId = fields[length -3];
		this.duration = StringUtil.convertInt(fields[length -2], 0);
		this.eventAttr = fields[length -1];
		
		setAttrMap(this.eventAttr);
		
		//如果是升级事件，用 online duration 代替 duration
		resetDuraForLevelUpEvent(eventId, fields);
	}
	
	public void setAttrMap(String eventAttr){
		arrtMap = new HashMap<String, String>();
		
		//如果事件属性值为 '-'，说明没有属性
		if(MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(eventAttr)){
			return;
		}
		
		try{
			Gson gson = new Gson();
			//arrtMap = gson.fromJson(eventAttr, HashMap.class);
			arrtMap = gson.fromJson(eventAttr, new TypeToken<Map<String, String>>(){}.getType());
			
			//20140728：由于下游 MR 的原因，这里需将值中 ':' 替换为 '_'
			for(Entry<String, String> entry : arrtMap.entrySet()){
				entry.setValue(entry.getValue().replaceAll(":", "_"));
			}
		}catch(Throwable t){
			String[] attrArr = eventAttr.split(",");
			for(String attr : attrArr){
				/*
				 * 20140718: 对 属性的 value 中又有冒号的情况进行兼容
				String[] keyVal = attr.split(":");
				if(keyVal.length > 1){
					arrtMap.put(keyVal[0], keyVal[1]);
				}
				*/
				int firstIndex = attr.indexOf(":");
				if(firstIndex > 0 && firstIndex != (attr.length()-1)){//冒号必须存在并且不是最后一个字符
					String key = attr.substring(0, firstIndex);
					String val = attr.substring(firstIndex+1).replaceAll(":", "_");
					arrtMap.put(key, val);
				}
			}
		}
	}
	
	/**
	 * 2014.01.22 新改需求，自定义事件协议有前后两个版本
	 * v1: header fields, extend fields, eventId, duration, eventAttr
	 * v2: header fields, extend fields, online duration, eventId, duration, eventAttr
	 * 
	 * 与 v1 相比，v2 增加了 online duration 这个字段，即事件发生的实际在线时长
	 * 如果当前事件是升级时间，同时  online duration 不为 0 且小于原来的 duration
	 * 则用 online duration 代替原有的 duration 用于升级时长统计
	 * @param eventId
	 * @param fields
	 */
	private void resetDuraForLevelUpEvent(String eventId, String[] fields){
		if(eventId.equals(Constants.EVENT_ID_LEVEL_UP)){
			if(fields.length >= 27){
				int onlineDuration = StringUtil.convertInt(fields[fields.length - 4], 0);
				duration = onlineDuration == 0 ? duration : Math.min(onlineDuration, duration);
			}
		}
	}
	
	/**
	 * 该方法为临时解决问题用
	 * @return
	 */
	public String[] toOldVersionArr(){
		String[] arr = new String[20];
		arr[0] = this.getTimestamp();
		arr[1] = this.getAppID();
		arr[2] = this.getUID();
		arr[3] = this.getAccountID();
		arr[4] = this.getPlatform();
		arr[5] = this.getChannel();
		arr[6] = this.getAccountType();
		arr[7] = this.getGender();
		arr[8] = this.getAge();
		arr[9] = this.getGameServer();
		arr[10] = this.getResolution();
		arr[11] = this.getOperSystem();
		arr[12] = this.getBrand();
		arr[13] = this.getNetType();
		arr[14] = this.getCountry();
		arr[15] = this.getProvince();
		arr[16] = this.getOperators();
		arr[17] = this.getEventId();
		arr[18] = ""+this.getDuration();
		arr[19] = this.getEventAttr();
		return arr;
	}

}
