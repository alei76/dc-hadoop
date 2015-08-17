package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.Calendar;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.OnlineDayLog2;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.reflect.TypeToken;

/**
 * 
 * 输入：经过滚存后的玩家信息
 * 
 * 统计新增活跃用户下列指标
 * 当然人数、登录次数、在线时长、国家地区机型等分布
 */
public class CHUserInfoLayoutMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private String[] one = new String[]{"1"};
	
	private Calendar cal = Calendar.getInstance();
	private TypeToken<Map<Integer, Integer>> type = new TypeToken<Map<Integer, Integer>>(){};
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineDayLog2 onlineDayLog2 = new OnlineDayLog2(paraArr);
		
		// 新增活跃玩家数、登录次数、在线时长
		String[] keyFields = new String[]{
				onlineDayLog2.getAppId(),
				onlineDayLog2.getAppVer(),
				onlineDayLog2.getPlatform(),
				onlineDayLog2.getChannel(),
				onlineDayLog2.getExtendsHelper().getCnty(),
				onlineDayLog2.getExtendsHelper().getProv(),
				Constants.PLAYER_TYPE_ONLINE
		};
		String[] valFields = new String[]{
				onlineDayLog2.getTotalLoginTimes()+"",
				onlineDayLog2.getTotalOnlineTime()+""
		};
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		keyObj.setSuffix(Constants.SUFFIX_CHANNEL_ONLINE_SUM);
		context.write(keyObj, valObj);
		if("Y".equals(onlineDayLog2.getIsNewPlayer())){
			keyFields[keyFields.length - 1] = Constants.PLAYER_TYPE_NEWADD;
			context.write(keyObj, valObj);
		}
		
		//机型分布
		String brand = onlineDayLog2.getExtendsHelper().getBrand();
		writeLayout(onlineDayLog2, Constants.DIM_TYPE_BRAND_DIS+"", brand, Constants.SUFFIX_CHANNEL_LAYOUT_EQUIP, context);
		
		//网络分布
		String netType = onlineDayLog2.getExtendsHelper().getNetwork(); //网络类型
		String netOp = onlineDayLog2.getExtendsHelper().getNetop(); // 宽带运营商
		String mobileOp = onlineDayLog2.getExtendsHelper().getMobileop(); // 移动运营商
		mobileOp = StringUtil.isEmpty(mobileOp) ? "-" : mobileOp;
		writeLayout(onlineDayLog2, Constants.DIM_TYPE_NET_TYPE_DIS+"", netType, Constants.SUFFIX_CHANNEL_LAYOUT_EQUIP, context);
		writeLayout(onlineDayLog2, Constants.DIM_TYPE_BROAD_OPER_DIS+"", netOp, Constants.SUFFIX_CHANNEL_LAYOUT_EQUIP, context);
		writeLayout(onlineDayLog2, Constants.DIM_TYPE_MOBILE_OPER_DIS+"", mobileOp, Constants.SUFFIX_CHANNEL_LAYOUT_EQUIP, context);
		
		//登录时段分布
		String onlineRecoeds = onlineDayLog2.getOnlineRecords();
		Map<Integer, Integer> map = StringUtil.getMapFromJson(onlineRecoeds, type);
		if(null != map && !map.isEmpty()){
			int minLoginTime = 0;
			for(int loginTime : map.keySet()){
				//计算得到一天登录的最小时间
				if(0 == minLoginTime){
					minLoginTime = loginTime;
				}
				minLoginTime = Math.min(loginTime, minLoginTime);
			}
			cal.setTimeInMillis(1000L*minLoginTime);
			int hour = cal.get(Calendar.HOUR_OF_DAY);
			writeLayout(onlineDayLog2, Constants.DIM_TYPE_LOGIN_HOUR_DIS+"", hour+"", Constants.SUFFIX_CHANNEL_LAYOUT_ONLINE, context);
		}
		
		//登录次数
		int loginTimes = onlineDayLog2.getTotalLoginTimes();
		int loginTimesRange = EnumConstants.getItval4DayLoginTimes(loginTimes);
		writeLayout(onlineDayLog2, Constants.DIM_TYPE_LOGIN_TIMES_DIS+"", loginTimesRange+"", Constants.SUFFIX_CHANNEL_LAYOUT_ONLINE, context);
		
		//用户城市分布
		String city = onlineDayLog2.getExtendsHelper().getCity();
		writeLayout(onlineDayLog2, Constants.DIM_TYPE_CITY_NUM_DIS+"", city, Constants.SUFFIX_CHANNEL_LAYOUT_ONLINE, context);
		
	}
	
	private void writeLayout(OnlineDayLog2 onlineDayLog2, String dimType, String vkey, String suffix, Context context) 
			throws IOException, InterruptedException{
		String[] paramArr = new String[]{
				onlineDayLog2.getAppId(),
				onlineDayLog2.getAppVer(),
				onlineDayLog2.getPlatform(),
				onlineDayLog2.getChannel(),
				onlineDayLog2.getExtendsHelper().getCnty(),
				onlineDayLog2.getExtendsHelper().getProv(),
				Constants.PLAYER_TYPE_ONLINE,
				dimType,
				vkey
		};
		keyObj.setOutFields(paramArr);
		valObj.setOutFields(one);
		keyObj.setSuffix(suffix);
		context.write(keyObj, valObj);
		if("Y".equals(onlineDayLog2.getIsNewPlayer())){
			paramArr[paramArr.length-3] = Constants.PLAYER_TYPE_NEWADD;
			context.write(keyObj, valObj);
		}
	}
}
