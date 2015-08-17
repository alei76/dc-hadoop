package net.digitcube.hadoop.tmp.datamining;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.jce.OnlineDay;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewAddPlayerDetailMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private Date date = new Date();
	private int day_20150108 = 1420646400;
	private int day_20150110 = 1420819200;
	Calendar cal = Calendar.getInstance();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoRollingLog user = new UserInfoRollingLog(date, paraArr);
		if(!user.getAppID().contains("084422A01CA099AD10E1889507756EA8")){
			return;
		}
		if(!MRConstants.ALL_GAMESERVER.equals(user.getPlayerDayInfo().getGameRegion())){
			return;
		}
		
		int newAddDay = user.getPlayerDayInfo().getFirstLoginDate();
		if(newAddDay < day_20150108 || newAddDay > day_20150110){
			return;
		}
		
		List<OnlineDay> list = user.getPlayerDayInfo().getOnlineDayList();
		if(null == list || list.isEmpty()){
			return;
		}
		String[] valFields = new String[]{
				"0","0","0","0","0","0","0","0","0","0","0","0"
		};
		for(OnlineDay day : list){
			if(newAddDay == day.getOnlineDate()){
				valFields[0] = day.getPayTimes()+"";
				valFields[1] = day.getPayAmount()+"";
				valFields[2] = day.getLoginTimes()+"";
				valFields[3] = day.getOnlineTime()+"";
			}else if((24*3600+newAddDay) == day.getOnlineDate()){
				valFields[4] = day.getPayTimes()+"";
				valFields[5] = day.getPayAmount()+"";
				valFields[6] = day.getLoginTimes()+"";
				valFields[7] = day.getOnlineTime()+"";
			}else if((2*24*3600+newAddDay) == day.getOnlineDate()){
				valFields[8] = day.getPayTimes()+"";
				valFields[9] = day.getPayAmount()+"";
				valFields[10] = day.getLoginTimes()+"";
				valFields[11] = day.getOnlineTime()+"";
			}
		}
		
		cal.setTimeInMillis(1000L*newAddDay);
		String newAdd = sdf.format(cal.getTime());
		String[] keyFields = new String[]{
				user.getAccountID(),
				newAdd
		};
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		keyObj.setSuffix(newAdd);
		context.write(keyObj, valObj);
	}
}
