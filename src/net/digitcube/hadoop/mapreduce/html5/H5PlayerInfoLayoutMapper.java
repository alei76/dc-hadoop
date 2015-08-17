package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class H5PlayerInfoLayoutMapper extends
			Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {	

	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private String fileName = "";

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);		
		if(fileName.contains(Constants.SUFFIX_H5_ONLINEDAY_FOR_PLAYER_DEVICE)){
			int i = 0;
			String appId = array[i++];	
			String platform = array[i++];	
			String h5PromotionAPP = array[i++];	
			String h5Domain = array[i++];	
			String h5Ref = array[i++];	
			String accountId = array[i++];			
			String playerType = array[i++];			
			String[] playerTypeArr = null;
			if(Constants.DATA_FLAG_PLAYER_NEW_ONLINE.equals(playerType)){
				playerTypeArr = new String[]{		
						Constants.PLAYER_TYPE_NEWADD, //新增
						Constants.PLAYER_TYPE_ONLINE //活跃
				};			
			}else if(Constants.DATA_FLAG_PLAYER_ONLINE.equals(playerType)){
				playerTypeArr = new String[]{
						Constants.PLAYER_TYPE_ONLINE //活跃
				};
			}			
			if(null == playerTypeArr){
				return;
			}
			String brand = array[i++];	
			String operSystem = array[i++];	
			String resolution = array[i++];	
			
			for(String playerTp : playerTypeArr){
				//1. 输出新增活跃付费玩家在设备及玩家属性上的分布统计
				//设备分布统计
				writeDeviceLayoutResult(context,
						appId,
						platform,
						h5PromotionAPP,
						h5Domain,
						h5Ref,
						playerTp,
						brand,
						operSystem,
						resolution
						);
			}
		}
	}

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
		mapKeyObj.setSuffix(Constants.SUFFIX_H5_LAYOUT_ON_DEVICE);

		mapKeyObj.setOutFields(brand);
		context.write(mapKeyObj, one);

		mapKeyObj.setOutFields(resolution);
		context.write(mapKeyObj, one);

		mapKeyObj.setOutFields(opersystem);
		context.write(mapKeyObj, one);

	}

}
