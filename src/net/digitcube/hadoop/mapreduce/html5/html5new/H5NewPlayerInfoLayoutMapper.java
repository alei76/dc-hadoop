package net.digitcube.hadoop.mapreduce.html5.html5new;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.html5new.vo.H5OnlineDayLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class H5NewPlayerInfoLayoutMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();
	private String fileName = "";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);		
		if(fileName.contains(Constants.SUFFIX_H5_NEW_PLAYER_ONLINE_INFO)){						
			H5OnlineDayLog onlineDayLog = new H5OnlineDayLog(array);
			String playerType = onlineDayLog.getPlayerType();
			
			String[] playerTypeArr = null;
			if(Constants.DATA_FLAG_PLAYER_NEW_ONLINE_PAY.equals(playerType)){
				playerTypeArr = new String[]{
						Constants.PLAYER_TYPE_NEWADD, //新增
						Constants.PLAYER_TYPE_ONLINE, //活跃
						Constants.PLAYER_TYPE_PAYMENT //付费
				};
			}else if(Constants.DATA_FLAG_PLAYER_NEW_ONLINE.equals(playerType)){
				playerTypeArr = new String[]{
						Constants.PLAYER_TYPE_NEWADD, //新增
						Constants.PLAYER_TYPE_ONLINE  //活跃
				};
			}else if(Constants.DATA_FLAG_PLAYER_ONLINE_PAY.equals(playerType)){
				playerTypeArr = new String[]{
						Constants.PLAYER_TYPE_ONLINE, //活跃
						Constants.PLAYER_TYPE_PAYMENT //付费
				};
			}else if(Constants.DATA_FLAG_PLAYER_ONLINE.equals(playerType)){
				playerTypeArr = new String[]{
						Constants.PLAYER_TYPE_ONLINE //活跃
				};
			}
			
			if(null == playerTypeArr){
				return;
			}
			for(String playerTp : playerTypeArr){				
				//1. 输出新增活跃付费玩家统计
				// 新增活跃统计
				writeNewOnlinePayResult(context,
						onlineDayLog.getAppId(),
						onlineDayLog.getPlatform(),
						onlineDayLog.getH5app(),
						onlineDayLog.getH5domain(),
						onlineDayLog.getH5ref(),
						playerTp,
						onlineDayLog.getTotalLoginTimes(),
						onlineDayLog.getTotalOnlineTime(),
						onlineDayLog.getIpRecords()
						);
				
				
				//2. 输出新增活跃付费玩家在设备及玩家属性上的分布统计
				// 设备分布统计
				writeDeviceLayoutResult(context,
						onlineDayLog.getAppId(),
						onlineDayLog.getPlatform(),
						onlineDayLog.getH5app(),
						onlineDayLog.getH5domain(),
						onlineDayLog.getH5ref(),
						playerTp,
						onlineDayLog.getBrand(),
						onlineDayLog.getResolution(),
						onlineDayLog.getOpSystem()
						);
			}
		}
	}

	//新增活跃统计
	private void writeNewOnlinePayResult(Context context, String appId,
			String platform, String h5app, String h5domain,String h5ref,
			String playerType, int totalLoginTimes, int totalOnlineTime,String ipRecords
			) throws IOException, InterruptedException {		
		//活跃玩家
		String[] keyFields = new String[] { 
				appId,
				platform,
				h5app,
				h5domain,
				h5ref,
				playerType
		};
		String[] valFields = new String[]{
				totalLoginTimes + "" ,
				totalOnlineTime + "",
				ipRecords
		};
		
		// 设置后缀为按设备分布
		mapKeyObj.setSuffix(Constants.SUFFIX_H5_NEW_ONLINE_PAY);
		mapKeyObj.setOutFields(keyFields);
		mapValueObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValueObj);
	}	
	
	//设备分布统计
	private void writeDeviceLayoutResult(Context context, String appId,
			String platfrom, String h5app, String h5domain,String h5ref,
			String playerType, String bd, String rl,String os
			) throws IOException, InterruptedException {

		// 机型
		String[] brand = new String[] { appId, platfrom, h5app, h5domain,h5ref,
				playerType, Constants.DIMENSION_DEVICE_BRAND,bd};

		// 分辨率
		String[] resolution = new String[] { appId, platfrom, h5app, h5domain,h5ref,
			    playerType, Constants.DIMENSION_DEVICE_RESOL,rl};

		// 操作系统
		String[] opersystem = new String[] {  appId, platfrom, h5app, h5domain,h5ref,
			    playerType, Constants.DIMENSION_DEVICE_OS,os};
	
		// 设置后缀为按设备分布
		mapKeyObj.setSuffix(Constants.SUFFIX_H5_NEW_LAYOUT_ON_DEVICE);
		mapKeyObj.setOutFields(brand);
		mapValueObj.setOutFields(new String[]{"1"});
		context.write(mapKeyObj, mapValueObj);

		mapKeyObj.setOutFields(resolution);
		context.write(mapKeyObj, mapValueObj);

		mapKeyObj.setOutFields(opersystem);
		context.write(mapKeyObj, mapValueObj);

	}

}
