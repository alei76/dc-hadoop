package net.digitcube.hadoop.mapreduce.userroll;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月19日 下午4:52:36 @copyrigt www.digitcube.net <br>
 * 输入:
 * 	AppID,Platform,Channel,GameRegion,Type,Level,accountId
 *  Type(L7/14/30,PL7/14/30,B7/14/30,PB7/14/30,S1/7/14/30)
 * <br>
 * 其中 
 * Level 字段的为 "流失玩家等级分布"统计需求新增
 * 20140528:accountId 为"TCL 流失玩家最后使用页面和功能"需求新增
 * 在 UserInfoRollingDay 中输出
 * 玩家流失、回流、留存只需用到 AppID,Platform,Channel,GameRegion,Type 字段
 * <br>
 * 输出：Key:APPID PlatForm channel gameregion Type value:1
 */

public class UserFlowMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		/*String[] outFields = new String[paraArr.length -1];
		System.arraycopy(paraArr, 0, outFields, 0, paraArr.length -1);*/
		int i = 0;
		String appId = paraArr[i++];
		String platform = paraArr[i++];
		String channel = paraArr[i++];
		String gameServer = paraArr[i++];
		String userFlowtype = paraArr[i++];
		String[] outFields = new String[]{
				appId,
				platform,
				channel,
				gameServer,
				userFlowtype
		};
		mapKeyObj.setOutFields(outFields);
		context.write(mapKeyObj, one);
	}
}
