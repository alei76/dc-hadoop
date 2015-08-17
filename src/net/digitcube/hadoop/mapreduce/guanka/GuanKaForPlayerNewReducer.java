package net.digitcube.hadoop.mapreduce.guanka;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.hbase.PlayerNewAddInfoHBaseMapper;
import net.digitcube.hadoop.mapreduce.hbase.util.HbasePool;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.PlayerType;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;

public class GuanKaForPlayerNewReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	//关卡的开始次数、成功完成次数、失败次数
	private Map<String, Integer> beginTimesMap = new HashMap<String, Integer>();
	private Map<String, Integer> successTimesMap = new HashMap<String, Integer>();
	private Map<String, Integer> failedTimesMap = new HashMap<String, Integer>();
	//失败退出率(levelId, map(loginTime, timeResult))
	private Map<String, Map<Integer,TimeAndResult>> failedLogoutMap = new HashMap<String, Map<Integer,TimeAndResult>>();
	//关卡完成时总时长（包括成功失败）
	private Map<String, Integer> totalDurationMap = new HashMap<String, Integer>();
	private Map<String, Integer> succDurationMap = new HashMap<String, Integer>();
	private Map<String, Integer> failDurationMap = new HashMap<String, Integer>();
	
	//所有关卡 id
	private Set<String> guankaIdSet = new HashSet<String>();
	
	private int statTime = 0;
	HConnection conn = null;
	HTableInterface htable = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statTime = DateUtil.getStatDate(context.getConfiguration());
		conn = HbasePool.getConnection(); 
		htable = conn.getTable(PlayerNewAddInfoHBaseMapper.TB_USER_NEWADD);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		HbasePool.close(conn);
	}
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		beginTimesMap.clear();
		successTimesMap.clear();
		failedTimesMap.clear();
		failedLogoutMap.clear();
		totalDurationMap.clear();
		succDurationMap.clear();
		failDurationMap.clear();
		guankaIdSet.clear();
		
		if(Constants.SUFFIX_GUANKA_FAIL_REASON_DIST.equals(key.getSuffix())){ // 关卡失败原因分布
			String[] fields = key.getOutFields();
			int i = 0;
			String appId = fields[i++];
			String platform = fields[i++];
			String channel = fields[i++];
			String gameServer = fields[i++];
			String accountId = fields[i++];
			String levelId = fields[i++];
			String failPoint = fields[i++];
			String[] playerTypes = searchPlayerTypeFromHBase(appId, platform, gameServer, accountId, statTime);
			int count = 0;
			for(OutFieldsBaseModel val : values){
				count ++;
			}
			key.setOutFields(new String[]{appId,platform,channel,gameServer,accountId,playerTypes[0],playerTypes[1],levelId,failPoint});
			valObj.setOutFields(new String[]{count+""});
			context.write(key, valObj);
		}else{
			//关卡完成的总耗时(不管关卡成功或失败)
			for(OutFieldsBaseModel val : values){
				String dataFlag = val.getOutFields()[0];
				String levelId = val.getOutFields()[1];
				//保存所有关卡 id
				guankaIdSet.add(levelId);
				
				if(Constants.DATA_FLAG_GUANKA_BEGIN.equals(dataFlag)){//begin times
					Integer count = beginTimesMap.get(levelId);
					if(null == count){
						beginTimesMap.put(levelId, 1);
					}else{
						beginTimesMap.put(levelId, count + 1);
					}
				}else if(Constants.DATA_FLAG_GUANKA_END.equals(dataFlag)){
					String result = val.getOutFields()[2];
					int endTime = StringUtil.convertInt(val.getOutFields()[3], 0);
					int loginTime = StringUtil.convertInt(val.getOutFields()[4], 0);
					int currentDuration = StringUtil.convertInt(val.getOutFields()[5], 0);
					
					//关卡完成时总时长
					Integer totalDuration = totalDurationMap.get(levelId);
					if(null == totalDuration){
						totalDurationMap.put(levelId, currentDuration);
					}else{
						totalDurationMap.put(levelId, currentDuration + totalDuration);
					}
					
					if(Constants.DATA_FLAG_EVENT_TRUE.equals(result)){
						Integer count = successTimesMap.get(levelId);
						if(null == count){
							successTimesMap.put(levelId, 1);
						}else{
							successTimesMap.put(levelId, count + 1);
						}
						
						//关卡成功时总时长
						Integer succTotalDuration = succDurationMap.get(levelId);
						if(null == succTotalDuration){
							succDurationMap.put(levelId, currentDuration);
						}else{
							succDurationMap.put(levelId, currentDuration + succTotalDuration);
						}
					}else if(Constants.DATA_FLAG_EVENT_FALSE.equals(result)){
						Integer count = failedTimesMap.get(levelId);
						if(null == count){
							failedTimesMap.put(levelId, 1);
						}else{
							failedTimesMap.put(levelId, count + 1);
						}
						
						//关卡失败时总时长
						Integer failTotalDuration = failDurationMap.get(levelId);
						if(null == failTotalDuration){
							failDurationMap.put(levelId, currentDuration);
						}else{
							failDurationMap.put(levelId, currentDuration + failTotalDuration);
						}
					}
					
					//loginTime,TimeAndResult
					Map<Integer,TimeAndResult> timeResultMap = failedLogoutMap.get(levelId);
					if(null == timeResultMap){
						timeResultMap = new HashMap<Integer,TimeAndResult>();
						timeResultMap.put(loginTime, new TimeAndResult(endTime, result));
						failedLogoutMap.put(levelId, timeResultMap);
					}else{
						TimeAndResult timeResult = timeResultMap.get(loginTime);
						if(null == timeResult){
							timeResultMap.put(loginTime, new TimeAndResult(endTime, result));
						}else{
							//每个 loginTime 只保存 endTime 最大的记录
							//若该记录为失败，则视为失败退出
							if(endTime > timeResult.getEndTime()){
								timeResult.setEndTime(endTime);
								timeResult.setResult(result);
							}
						}
					}
				}
			}
			writeResult(key, context);
		}
	}
	
	private void writeResult(OutFieldsBaseModel key, Context context) throws IOException, InterruptedException{
		key.setSuffix(Constants.SUFFIX_GUANKA_FOR_PLAYER);
		String[] fields = key.getOutFields();
		int i = 0;
		String appId = fields[i++];
		String platform = fields[i++];
		String channel = fields[i++];
		String gameServer = fields[i++];
		String accountId = fields[i++];
		
		String[] playerType = searchPlayerTypeFromHBase(appId, platform, gameServer, accountId, statTime);
		
		//对玩家当天的每个关卡输出一条记录，统计信息包括：
		//开始次数，成功次数，失败次数，失败退出次数，总时长，成功总时长，失败总时长
		for(String guankaId : guankaIdSet){
			
			int beginTimes = getValFromMap(beginTimesMap, guankaId, 0); //beginTimes
			int successTimes = getValFromMap(successTimesMap, guankaId, 0); //successTimes
			int failedTimes = getValFromMap(failedTimesMap, guankaId, 0); //failedTimes
			int failedExitTimes = 0; //failedExitTimes
			Map<Integer,TimeAndResult> timeResultMap = failedLogoutMap.get(guankaId);
			if(null != timeResultMap){
				Set<Entry<Integer,TimeAndResult>> loginTimeSet = timeResultMap.entrySet();
				//entry = loginTime, timeResult
				for(Entry<Integer,TimeAndResult> entry : loginTimeSet){
					//每个 loginTime 对应的最后记录若为失败，则视为失败退出
					TimeAndResult timeAndResult = entry.getValue();
					if(Constants.DATA_FLAG_EVENT_FALSE.equals(timeAndResult.getResult())){
						failedExitTimes++;
					}
				}
			}
			
			int duration = getValFromMap(totalDurationMap, guankaId, 0); //total duration
			int succDuration = getValFromMap(succDurationMap, guankaId, 0); //success duration
			int failDuration = getValFromMap(failDurationMap, guankaId, 0); //failed duration
			
			String[] valFields = new String[]{
					playerType[0], // isNewPlayer
					playerType[1], // isPayPlayer
					guankaId,
					beginTimes + "",
					successTimes + "",
					failedTimes + "",
					failedExitTimes + "",
					duration + "",
					succDuration + "",
					failDuration + ""
			};
			
			valObj.setOutFields(valFields);
			context.write(key, valObj);
		}
	}
	
	private int getValFromMap(Map<String, Integer> map, String guankaId, int defaultVal){
		Integer val = map.get(guankaId);
		if(null == val){
			return defaultVal;
		}
		return val;
	}
	
	private String[] searchPlayerTypeFromHBase(String appId, String platform, String gameServer, String accountId, int statTime){
		String[] playerType = new String[]{Constants.DATA_FLAG_NO, Constants.DATA_FLAG_NO};
		String pureAppId = appId.split("\\|")[0];
		String key = pureAppId + "|" + platform + "|" + gameServer + "|" + accountId;
		try {
			Get get = new Get(Bytes.toBytes(key));
			Result result = htable.get(get);
			
			//符合下列条件之一时视为新增玩家
			//a,表中不存在(当天小时任务计算时表中还不存在，次日凌晨  MR 任务才会把玩家加入到表中)
			//b,表中存在,并且新增日期和统计日期相同
			if(null == result){
				playerType[0] = Constants.DATA_FLAG_YES; //new player
			}else {
				byte[] firstLoginDay = result.getValue(PlayerNewAddInfoHBaseMapper.info, PlayerNewAddInfoHBaseMapper.fstLgDay);
				byte[] firstPayDay = result.getValue(PlayerNewAddInfoHBaseMapper.info, PlayerNewAddInfoHBaseMapper.fstPayDay);
				
				if(null != firstLoginDay && statTime == Bytes.toInt(firstLoginDay)){
					playerType[0] = Constants.DATA_FLAG_YES; //new player
				}
				
				//付费玩家条件是表中必须存在，并且 firstPayDay > 0
				//当天第一次付费的玩家在表中不能及时体现
				//所以小时任务判断时，这部分付费玩家会计算不到
				if(null !=firstPayDay && Bytes.toInt(firstPayDay) > 0){
					playerType[1] = Constants.DATA_FLAG_YES; //pay player
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return playerType;
	}
	
	public static PlayerType searchPlayerTypeFromHBase(HTableInterface htable,
			String appId, 
			String platform, 
			String gameServer, 
			String accountId, 
			int statTime){
		
		PlayerType playerType = new PlayerType();
		
		String pureAppId = appId.split("\\|")[0];
		String key = pureAppId + "|" + platform + "|" + gameServer + "|" + accountId;
		try {
			Get get = new Get(Bytes.toBytes(key));
			Result result = htable.get(get);
			
			//符合下列条件之一时视为新增玩家
			//a,表中不存在(当天小时任务计算时表中还不存在，次日凌晨  MR 任务才会把玩家加入到表中)
			//b,表中存在,并且新增日期和统计日期相同
			if(null == result){
				playerType.markNewAdd(); //new player
			}else {
				byte[] firstLoginDay = result.getValue(PlayerNewAddInfoHBaseMapper.info, PlayerNewAddInfoHBaseMapper.fstLgDay);
				byte[] firstPayDay = result.getValue(PlayerNewAddInfoHBaseMapper.info, PlayerNewAddInfoHBaseMapper.fstPayDay);
				
				if(null != firstLoginDay && statTime == Bytes.toInt(firstLoginDay)){
					playerType.markNewAdd(); //new player.markNewAdd(); //new player
				}
				
				//付费玩家条件是表中必须存在，并且 firstPayDay > 0
				//当天第一次付费的玩家在表中不能及时体现
				//所以小时任务判断时，这部分付费玩家会计算不到
				if(null !=firstPayDay && Bytes.toInt(firstPayDay) > 0){
					playerType.markEverPay(); //pay player
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return playerType;
	}
	
	class TimeAndResult{
		int endTime;
		String result;
		
		private TimeAndResult(int endTime, String result) {
			this.endTime = endTime;
			this.result = result;
		}
		public int getEndTime() {
			return endTime;
		}
		public void setEndTime(int endTime) {
			this.endTime = endTime;
		}
		public String getResult() {
			return result;
		}
		public void setResult(String result) {
			this.result = result;
		}
	}
}
