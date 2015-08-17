package net.digitcube.hadoop.mapreduce.taskanditem;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PlayerInfoForExtMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	
	// 当前输入的文件后缀
	private String fileSuffix = "";
	// 统计的数据时间
	private int statTime = 0;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		statTime = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);		
		if (fileSuffix.contains(Constants.SUFFIX_EXT_INFO_ROLL_DAY)) { //滚存输出
			if(array.length != 6){
				return;
			}
			String appID = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			String accountId = array[4];
			String detailInfo = array[5];
			//20141218:暂时丢掉过大的数据，下周改掉
			if(detailInfo.getBytes("utf-8").length > 65536){
				return;
			}
			String[] keyFields = new String[] { appID, platform, gameServer, accountId };
			String[] valFields = new String[] { channel,detailInfo};
			mapValObj.setSuffix("EXTROLL");
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);			
			
		}else if(fileSuffix.contains(Constants.SUFFIX_PLAYER_NEWADD_AND_PAY)){	// 滚存日志			
			String appID = array[0];
			String platform = array[1];
			String channel = array[2];
			String gameServer = array[3];
			String accountId = array[4];
			String firstLoginDate = array[5];
			String totalPayCur = array[6];
			String payTrack = array[7];
			String[] keyFields = new String[] { appID, platform, gameServer, accountId };
			String[] valFields = new String[] { firstLoginDate, totalPayCur};
			mapValObj.setSuffix("PLAYERNEWPAY");
			mapKeyObj.setOutFields(keyFields);
			mapValObj.setOutFields(valFields);
			context.write(mapKeyObj, mapValObj);			
		}	
		
	}
	
	

}
