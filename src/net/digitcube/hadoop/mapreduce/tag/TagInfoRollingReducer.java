package net.digitcube.hadoop.mapreduce.tag;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.mapreduce.tag.TagInfoLog.TagInfo;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TagInfoRollingReducer extends Reducer<OutFieldsBaseModel, BigFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	// 取结算日期(数据发生日期)
	private int statDate = 0;

	private TreeSet<TagAction> set = new TreeSet<TagAction>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		if(context.getConfiguration().getBoolean("is.hour.job", false)){
			//小时任务:结算时间取当天凌晨零点
			statDate = DateUtil.getStatDateForToday(context.getConfiguration());
		}else{
			//天任务:结算时间取昨天凌晨零点
			statDate = DateUtil.getStatDate(context.getConfiguration());
		}
	}
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<BigFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		set.clear();
		
		String aChannel = "";
		String maxVersion = "";
		String[] tagRollArr = null;
		String[] onlineDayArr = null;
		String[] paymentDayArr = null;
		String[] lostDaysArr = null;
		OnlineDayLog onlineDayLog = null;
		PaymentDayLog paymentDayLog = null;
		
		boolean isHaveTagInfo = false;
		for(BigFieldsBaseModel val : values){
			int i = 0;
			if(TagInfoRollingMapper.DATA_FLAG_ROLL.equals(val.getSuffix())){
				isHaveTagInfo = true;
				tagRollArr = val.getOutFields();
			} else if(TagInfoRollingMapper.DATA_FLAG_ACTION.equals(val.getSuffix())){
				isHaveTagInfo = true;
				String version = val.getOutFields()[i++];
				String channel = val.getOutFields()[i++];
				String tagName = val.getOutFields()[i++];
				String subTagName = val.getOutFields()[i++];
				String action = val.getOutFields()[i++];
				String actionTime = val.getOutFields()[i++];
				
				if(version.compareTo(maxVersion) > 0){
					maxVersion = version; 
				}
				aChannel = channel;
				
				set.add(new TagAction(tagName, subTagName, action, actionTime));
				
			} else if(TagInfoRollingMapper.DATA_FLAG_ONLINE.equals(val.getSuffix())){
				
				onlineDayArr = val.getOutFields();
				
			} else if(TagInfoRollingMapper.DATA_FLAG_PAYMENT.equals(val.getSuffix())){
				
				paymentDayArr = val.getOutFields();
			} else if(TagInfoRollingMapper.DATA_FLAG_PLAYER_LOST.equals(val.getSuffix())){
				lostDaysArr = val.getOutFields();
			}
		}
		
		if(!isHaveTagInfo){
			return;
		}
		
		int j = 0;
		TagInfoLog tagInfoLog = null;
		if(null != tagRollArr){
			tagInfoLog = new TagInfoLog(tagRollArr);
		}else{
			tagInfoLog = new TagInfoLog();
			tagInfoLog.setAppId(key.getOutFields()[j++]);
			tagInfoLog.setPlatform(key.getOutFields()[j++]);
			tagInfoLog.setGameServer(key.getOutFields()[j++]);
			tagInfoLog.setAccountId(key.getOutFields()[j++]);
			
			tagInfoLog.setAppVer(maxVersion);
			tagInfoLog.setChannel(aChannel);
		}
		
		//设置付费信息
		if(null != paymentDayArr){
			paymentDayLog = new PaymentDayLog(paymentDayArr);
			String[] appIdAndVer = paymentDayLog.getAppID().split("\\|");
			String version = appIdAndVer[1];
			//MARK PAY
			tagInfoLog.markPay(paymentDayLog.getTotalPayTimes() > 0);
			//设置最大版本和最新渠道
			if(version.compareTo(tagInfoLog.getAppVer()) > 0){
				tagInfoLog.setAppVer(version);
			}
			tagInfoLog.setChannel(paymentDayLog.getExtend().getChannel());
		}
		//设置在线信息
		if(null != onlineDayArr){
			onlineDayLog = new OnlineDayLog(onlineDayArr);
			String[] appIdAndVer = onlineDayLog.getAppID().split("\\|");
			String version = appIdAndVer[1];
			//记录在线信息及设置最大等级
			tagInfoLog.setLevel(onlineDayLog.getMaxLevel());
			tagInfoLog.markLogin(true);
			//设置最大版本和最新渠道
			if(version.compareTo(tagInfoLog.getAppVer()) > 0){
				tagInfoLog.setAppVer(version);
			}
			tagInfoLog.setChannel(onlineDayLog.getExtend().getChannel());
		}/*else if(set.size() > 0){
			//有标签添加或删除动作，说明有在线，记录在线信息
			//注意：如果 CP 通过 api 方式给玩家打标签的话
			//那么有标签动作玩家也不一定登录游戏
			tagInfoLog.markLogin(true);
		}*/
		
		//应用标签添加或删除动作
		if(set.size() > 0){
			doTagAction(tagInfoLog);
		}
		
		//A. 输出一级、二级标签的存量、新增、删除等信息
		statTagsStayAddRm(context, tagInfoLog, tagInfoLog.getTagsMap());
		
		//B. 输出一级、二级标签玩家的在线信息
		if(null != onlineDayLog){
			statTagPlayerOnline(context, 
					tagInfoLog, 
					tagInfoLog.getTagsMap(),
					onlineDayLog.getTotalLoginTimes(), 
					onlineDayLog.getTotalOnlineTime());
		}
		
		//C. 输出一级、二级标签玩家的付费信息
		if(null != paymentDayLog){
			statTagPlayerPay(context, 
					tagInfoLog, 
					tagInfoLog.getTagsMap(),
					paymentDayLog.getTotalPayTimes(), 
					paymentDayLog.getCurrencyAmount());
		}
		
		//D. 输出一级、二级标签玩家的留存信息
		if(null != onlineDayLog){
			statTagPlayerRetain(context, tagInfoLog, tagInfoLog.getTagsMap());
		}
		
		//E. 输出一级、二级标签的流失玩家信息
		if(null != lostDaysArr){
			String lostLevel = lostDaysArr[0];
			String lostDays = lostDaysArr[1];
			statTagPlayerLost(context, tagInfoLog, tagInfoLog.getTagsMap(), lostLevel, lostDays);
		}
		
		
		//F. 输出玩家标签滚存信息
		keyObj.setOutFields(tagInfoLog.toStringArray());
		keyObj.setSuffix(Constants.SUFFIX_TAG_ROLLING_DAY);
		context.write(keyObj, NullWritable.get());
	}
	
	/**
	 * 添加或删除标签
	 * @param tagInfoLog
	 */
	private void doTagAction(TagInfoLog tagInfoLog){
		for(TagAction action : set){
			if(TagInfoRollingMapper.DATA_FLAG_ADD_TAG.equals(action.action)){
				if(MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(action.subTagName)){
					tagInfoLog.addTag(action.tagName, statDate);
				}else{
					tagInfoLog.addSubTag(action.tagName, action.subTagName, statDate);
				}
			}else if(TagInfoRollingMapper.DATA_FLAG_RM_TAG.equals(action.action)){
				if(MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(action.subTagName)){
					tagInfoLog.removeTag(action.tagName, statDate);
				}else{
					tagInfoLog.removeSubTag(action.tagName, action.subTagName, statDate);
				}
			}
		}
	}
	
	/**
	 * 输出一级、二级标签的新增、删除等信息
	 * 
	 * @param context
	 * @param tagInfoLog
	 * @param tagsMap
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void statTagsStayAddRm(Context context, TagInfoLog tagInfoLog, Map<String, TagInfo> tagsMap) throws IOException, InterruptedException{
		for(Entry<String, TagInfo> entry : tagsMap.entrySet()){
			TagInfo tagInfo = entry.getValue();
			boolean isActive = tagInfo.isActive();
			boolean isNewAdd = (tagInfo.getCreateTime() == statDate);
			boolean isRemove = (tagInfo.getRemoveTime() == statDate && !tagInfo.isActive());
			String parentName = StringUtil.isEmpty(tagInfo.getParName()) ? Constants.TAG_DEFAULT_ID : tagInfo.getParName(); 
			String[] keyFields = new String[]{
					tagInfoLog.getAppId(),
					tagInfoLog.getAppVer(),
					tagInfoLog.getPlatform(),
					tagInfoLog.getChannel(),
					tagInfoLog.getGameServer(),
					tagInfoLog.getAccountId(),
					parentName,
					tagInfo.getTagName(),
					isActive+"",
					isNewAdd+"",
					isRemove+""
					
			};
			
			keyObj.setOutFields(keyFields);
			keyObj.setSuffix(Constants.SUFFIX_TAG_STAY_ADD_RM);
			context.write(keyObj, NullWritable.get());
			
			//recursively output subtag info
			statTagsStayAddRm(context, tagInfoLog, tagInfo.getSubTagsMap());
		}
	}
	
	private void statTagPlayerOnline(Context context, TagInfoLog tagInfoLog, Map<String, TagInfo> tagsMap,
			int loginTimes, int onlineTime) throws IOException, InterruptedException{
		for(Entry<String, TagInfo> entry : tagsMap.entrySet()){
			TagInfo tagInfo = entry.getValue();
			//如果已经删除并且不是当天删除的话则跳过
			if(!tagInfo.isActive() && tagInfo.getRemoveTime() != statDate){
				continue;
			}	
			
			String parentName = StringUtil.isEmpty(tagInfo.getParName()) ? Constants.TAG_DEFAULT_ID : tagInfo.getParName(); 
			String[] keyFields = new String[]{
					tagInfoLog.getAppId(),
					tagInfoLog.getAppVer(),
					tagInfoLog.getPlatform(),
					tagInfoLog.getChannel(),
					tagInfoLog.getGameServer(),
					tagInfoLog.getAccountId(),
					parentName,
					tagInfo.getTagName(),
					loginTimes+"",
					onlineTime+"",
					tagInfoLog.getLevel()+""
			};
			
			keyObj.setOutFields(keyFields);
			keyObj.setSuffix(Constants.SUFFIX_TAG_ONLINE_DAY);
			context.write(keyObj, NullWritable.get());
			
			//recursively output subtag info
			statTagPlayerOnline(context, tagInfoLog, tagInfo.getSubTagsMap(), loginTimes, onlineTime);
		}
	}
	
	private void statTagPlayerPay(Context context, TagInfoLog tagInfoLog, Map<String, TagInfo> tagsMap,
			int payTimes, float payAmount) throws IOException, InterruptedException{
		for(Entry<String, TagInfo> entry : tagsMap.entrySet()){
			TagInfo tagInfo = entry.getValue();
			//如果已经删除并且不是当天删除的话则跳过
			if(!tagInfo.isActive() && tagInfo.getRemoveTime() != statDate){
				continue;
			}
			String parentName = StringUtil.isEmpty(tagInfo.getParName()) ? Constants.TAG_DEFAULT_ID : tagInfo.getParName(); 
			String[] keyFields = new String[]{
					tagInfoLog.getAppId(),
					tagInfoLog.getAppVer(),
					tagInfoLog.getPlatform(),
					tagInfoLog.getChannel(),
					tagInfoLog.getGameServer(),
					tagInfoLog.getAccountId(),
					parentName,
					tagInfo.getTagName(),
					payTimes+"",
					payAmount+""
			};
			
			keyObj.setOutFields(keyFields);
			keyObj.setSuffix(Constants.SUFFIX_TAG_PAYMENT_DAY);
			context.write(keyObj, NullWritable.get());
			
			//recursively output subtag info
			statTagPlayerPay(context, tagInfoLog, tagInfo.getSubTagsMap(), payTimes, payAmount);
		}
	}
	
	private void statTagPlayerRetain(Context context, TagInfoLog tagInfoLog, Map<String, TagInfo> tagsMap) throws IOException, InterruptedException{
		for(Entry<String, TagInfo> entry : tagsMap.entrySet()){
			TagInfo tagInfo = entry.getValue();
			//如果已经删除并且不是当天删除的话则跳过
			if(!tagInfo.isActive() && tagInfo.getRemoveTime() != statDate){
				continue;
			}
			
			//当天必须有登录
			if (!tagInfoLog.isLogin(statDate, statDate)) {
				return;
			}
			
			//如果标签为当天新增，则无所谓留存
			if(tagInfo.getCreateTime() == statDate){
				return;
			}
			String parentName = StringUtil.isEmpty(tagInfo.getParName()) ? Constants.TAG_DEFAULT_ID : tagInfo.getParName();
			String[] keyFields = new String[38];
			int j = 0;
			keyFields[j++] = tagInfoLog.getAppId();
			keyFields[j++] = tagInfoLog.getAppVer();
			keyFields[j++] = tagInfoLog.getPlatform();
			keyFields[j++] = tagInfoLog.getChannel();
			keyFields[j++] = tagInfoLog.getGameServer();
			keyFields[j++] = tagInfoLog.getAccountId();
			keyFields[j++] = parentName;
			keyFields[j++] = tagInfo.getTagName();
			
			int offset = j;
			int playerType = 0;
			int i = 31;
			while ((--i) > 0) {
				int targetDate = statDate - i * 24 * 3600;
				
				//留存行为的统计必须是在打上标签之日起
				//当 targetDate 比打标签时间更前时则跳过
				if(targetDate < tagInfo.getCreateTime()){
					keyFields[offset + i - 1] = playerType+"";
					continue;
				}
				
				//targetDate 当天没有登录则跳过
				boolean isLogin = tagInfoLog.isLogin(targetDate, statDate);
				if (!isLogin) {
					keyFields[offset + i - 1] = playerType+"";
					continue;
				}
				
				// 活跃标签玩家
				playerType = playerType | 1;
				
				// 新打标签玩家
				boolean isTagOnExpectDay = tagInfo.getCreateTime() == targetDate;
				if(isTagOnExpectDay){
					playerType = playerType | 2;
				}
				// 第 i 天是否付费
				boolean isPayOnExpectDay = tagInfoLog.isPay(targetDate, statDate);
				if(isPayOnExpectDay){
					playerType = playerType | 4;
				}
				
				keyFields[offset + i - 1] = playerType+"";
			}
			 
			keyObj.setOutFields(keyFields);
			keyObj.setSuffix(Constants.SUFFIX_TAG_PLAYER_RETAIN);
			context.write(keyObj, NullWritable.get());
			
			//recursively output subtag info
			statTagPlayerRetain(context, tagInfoLog, tagInfo.getSubTagsMap());
		}
	}
	
	private void statTagPlayerLost(Context context, TagInfoLog tagInfoLog, Map<String, TagInfo> tagsMap,
			String lostLevel, String lostDays) throws IOException, InterruptedException{
		for(Entry<String, TagInfo> entry : tagsMap.entrySet()){
			TagInfo tagInfo = entry.getValue();
			//如果已经删除并且不是当天删除的话则跳过
			if(!tagInfo.isActive() && tagInfo.getRemoveTime() != statDate){
				continue;
			}
			String parentName = StringUtil.isEmpty(tagInfo.getParName()) ? Constants.TAG_DEFAULT_ID : tagInfo.getParName(); 
			String[] keyFields = new String[]{
					tagInfoLog.getAppId(),
					tagInfoLog.getAppVer(),
					tagInfoLog.getPlatform(),
					tagInfoLog.getChannel(),
					tagInfoLog.getGameServer(),
					tagInfoLog.getAccountId(),
					parentName,
					tagInfo.getTagName(),
					lostLevel,
					lostDays
			};
			
			keyObj.setOutFields(keyFields);
			keyObj.setSuffix(Constants.SUFFIX_TAG_PLAYER_LOST);
			context.write(keyObj, NullWritable.get());
			
			//recursively output subtag info
			statTagPlayerLost(context, tagInfoLog, tagInfo.getSubTagsMap(), lostLevel, lostDays);
		}
	}
	
	static class TagAction implements Comparable<TagAction>{
		String tagName;
		String subTagName;
		String action;
		String actionTime;
		private TagAction(String tagName, String subTagName, String action, String actionTime) {
			this.tagName = tagName;
			this.subTagName = subTagName;
			this.action = action;
			this.actionTime = actionTime;
		}
		@Override
		public int compareTo(TagAction o) {
			int result = this.actionTime.compareTo(o.actionTime);
			if(0 != result){
				return result;
			}
			
			result = this.tagName.compareTo(o.tagName);
			if(0 != result){
				return result;
			}
			
			result = this.subTagName.compareTo(o.subTagName);
			if(0 != result){
				return result;
			}
			
			result = this.action.compareTo(o.action);
			if(0 != result){
				return result;
			}
			
			return 0;
		}
		@Override
		public String toString() {
			return "TagAction [tagName=" + tagName + ", subTagName="
					+ subTagName + ", action=" + action + ", actionTime="
					+ actionTime + "]";
		}
	}
}
