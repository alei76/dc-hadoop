package test.payment;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PayIntervalTestReducer
		extends
		Reducer<Text, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel reduceValueObj = new OutFieldsBaseModel();

	// startTime/endTime 用于判断当天登录时间的有效性
	// 并用这些登录时间计算首充、二充、三充的时间间隔
	private int startTime = 0;
	private int endTime = 0;
	Calendar cal = null;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
	StringBuilder sb = new StringBuilder();
	
	Map<String, Integer> map = new HashMap<String, Integer>();
			
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		map.clear();
		cal = Calendar.getInstance();
		Date date = ConfigManager.getInitialDate(context.getConfiguration());
		if (date != null) {
			cal.setTime(date);
		}
		cal.set(Calendar.DAY_OF_MONTH, 11);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		endTime = (int)(cal.getTimeInMillis()/1000);
		
		//开始时间设置为前一天的晚上 8 点
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.add(Calendar.HOUR_OF_DAY, -4);
		startTime = (int)(cal.getTimeInMillis()/1000);
	}
	
	@Override
	protected void reduce(Text key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		String onlineRecords = null;
		String payRecords = null;
		
		int historyOnlineTime = 0;
		
		for(OutFieldsBaseModel val : values){
			String[] v = val.getOutFields();
			if("pay".equals(v[1])){
				payRecords = v[2];
			}else if("roll".equals(v[1])){
				historyOnlineTime = Integer.valueOf(v[2]);
			}else{
				onlineRecords = v[2];
			}
		}
		
		// onlineRecord 里记录了该玩家当天所有登录时间以及对应的在线时长
		//String[] onlineRecords = onlineArray[onlineArray.length - 1].split(",");
		String[] records = onlineRecords.split(",");
		TreeMap<Integer, Integer> onlineMap = new TreeMap<Integer, Integer>();
		for(String record : records){
			String[] arr = record.split(":");
			if(arr.length > 1){
				int loginTime = StringUtil.convertInt(arr[0], 0);
				int onlineTime = StringUtil.convertInt(arr[1], 0);
				// 登录时间不合法，则过滤
				if(loginTime <= 0 || onlineTime <= 0 || loginTime < startTime || loginTime > endTime){
					continue;
				}
				
				onlineMap.put(loginTime, onlineTime);
			}
		}
		/*if(0 == onlineMap.size()){
			return;
		}*/
		
		// paymentRecord 里记录了该玩家当天前三次的付费时间以及对应的等级
		// (目前业务需求值统计前三次充值时间间隔)
		String[] paymentRecords = payRecords.split(",");
		TreeMap<Integer, String> paymentMap = new TreeMap<Integer, String>();
		for(String record : paymentRecords){
			String[] arr = record.split(":");
			int payTime = StringUtil.convertInt(arr[0], 0);
			if(payTime <= 0){
				continue;
			}
			
			paymentMap.put(payTime, record);
		}
		if(0 == paymentMap.size()){
			return;
		}
		
		//TODO : FOR TEST
		sb.delete(0, sb.length());
		sb.append(key).append("\nonline:[");
		
		Set<Integer> loginTimeSet = onlineMap.keySet();
		for(Integer loginTime : loginTimeSet){
			cal.setTimeInMillis(1000L * (long)loginTime);
			String lt = sdf.format(cal.getTime());
			sb.append(lt).append("-->").append(onlineMap.get(loginTime)).append(", ");
		}
		sb.append("]\npaymnt:[");
		Set<Integer> payTimeSet1 = paymentMap.keySet();
		for(Integer payTime : payTimeSet1){
			cal.setTimeInMillis(1000L * (long)payTime);
			String lt = sdf.format(cal.getTime());
			sb.append(lt).append(", ");
		}
		sb.append("]");
		//END TEST
		
		// 分别计算每次付费距离当天首登的时长
		Set<Integer> payTimeSet = paymentMap.keySet();
		int[] payTimeArr = new int[payTimeSet.size() > 3 ? 3 : payTimeSet.size()];
		int i=0;
		for(int payTime : payTimeSet){
			if(i >= payTimeArr.length){
				break;
			}
			
			// 每次付费在当天里的在线时长(时长间隔 = payTimeArr[i] - payTimeArr[i-1])
			payTimeArr[i] = calculatePayTime(onlineMap,payTime);
			i++;
		}
		
		int firstPayTimeInterval = 0;
		int secondPayTimeInterval = 0;
		int thirdPayTimeInterval = 0;
		
		if(payTimeArr.length > 2){
			firstPayTimeInterval = historyOnlineTime + payTimeArr[0];
			secondPayTimeInterval = payTimeArr[1] - payTimeArr[0];
			thirdPayTimeInterval = payTimeArr[2] - payTimeArr[1];
			
			sb.append("\npayItv:[")
			.append(EnumConstants.getPayTimeRange(firstPayTimeInterval)).append(",")
			.append(EnumConstants.getPayTimeRange(secondPayTimeInterval)).append(",")
			.append(EnumConstants.getPayTimeRange(thirdPayTimeInterval))
			.append("]");
			
			String key1 = "1_" + EnumConstants.getPayTimeRange(firstPayTimeInterval);
			String key2 = "2_" + EnumConstants.getPayTimeRange(secondPayTimeInterval);
			String key3 = "3_" + EnumConstants.getPayTimeRange(thirdPayTimeInterval);
			Integer count = map.get(key1);
			if(null == count){
				map.put(key1, 1);
			}else{
				map.put(key1, 1+count);
			}
			count = map.get(key2);
			if(null == count){
				map.put(key2, 1);
			}else{
				map.put(key2, 1+count);
			}
			count = map.get(key3);
			if(null == count){
				map.put(key3, 1);
			}else{
				map.put(key3, 1+count);
			}
		}else if(payTimeArr.length > 1){
			firstPayTimeInterval = historyOnlineTime + payTimeArr[0];
			secondPayTimeInterval = payTimeArr[1] - payTimeArr[0];
			
			sb.append("\npayIntv:[")
			.append(EnumConstants.getPayTimeRange(firstPayTimeInterval)).append(",")
			.append(EnumConstants.getPayTimeRange(secondPayTimeInterval))
			.append("]");
			
			
			String key1 = "1_" + EnumConstants.getPayTimeRange(firstPayTimeInterval);
			String key2 = "2_" + EnumConstants.getPayTimeRange(secondPayTimeInterval);
			Integer count = map.get(key1);
			if(null == count){
				map.put(key1, 1);
			}else{
				map.put(key1, 1+count);
			}
			count = map.get(key2);
			if(null == count){
				map.put(key2, 1);
			}else{
				map.put(key2, 1+count);
			}
		}else if(payTimeArr.length == 1){
			firstPayTimeInterval = historyOnlineTime + payTimeArr[0];
			
			sb.append("\npayIntv:[")
			.append(EnumConstants.getPayTimeRange(firstPayTimeInterval))
			.append("]");
			
			String key1 = "1_" + EnumConstants.getPayTimeRange(firstPayTimeInterval);
			Integer count = map.get(key1);
			if(null == count){
				map.put(key1, 1);
			}else{
				map.put(key1, 1+count);
			}
		}
		
		System.out.println(sb.toString());
		System.out.println("---------------------------------------------------------------------");
	}

	private int calculatePayTime(TreeMap<Integer, Integer> onlineMap, int payTime){
		Integer floorLoginTime = onlineMap.floorKey(payTime);
		if(null == floorLoginTime){
			return 0;
		}
		
		int todayOnlineTime = 0;
		Set<Integer> loginTimeSet = onlineMap.keySet();
		for(Integer loginTime : loginTimeSet){
			if(loginTime >= floorLoginTime){
				break;
			}
			todayOnlineTime += onlineMap.get(loginTime);
		}
		
		todayOnlineTime = todayOnlineTime + (payTime - floorLoginTime); 
		return todayOnlineTime;
	}

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
		System.out.println("===================================================================================");
		Set<Entry<String, Integer>> set = map.entrySet();
		for(Entry<String, Integer> s:set){
			System.out.println(s.getKey() + ": " +s.getValue());
		}
	}
	
	
}
