package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 输入：@PageTreeForLoginTimeMapper
 * appId, platform, channel, gameServer, pageName, pageId, parentId, viewTimes, duration
 * 
 * 主要逻辑： 对所有玩家同一次登录排序好的访问页面构建一棵页面访问树
 * a) 统计每个页面的访问次数及时长
 * b) 统计每条路径中访问次数
 */
public class PageTreeSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private Text valObj = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		valObj.clear();
		
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String platform = arr[i++];
		String channel = arr[i++];
		String gameServer = arr[i++];
		String accountId = arr[i++];
		String loginTime = arr[i++];
		String pages = arr[i++];
		
		String[] keyFields = new String[]{
				appId,
				platform,
				channel,
				gameServer
		};
		
		keyObj.setOutFields(keyFields);
		valObj.set(pages);
		context.write(keyObj, valObj);
	}
}
