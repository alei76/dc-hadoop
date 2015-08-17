package net.digitcube.hadoop.mapreduce.gameexperience;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  对所有玩家同一次登录排序好的访问场景构建一棵场景访问树  
 *  a) 统计每个场景的访问次数及时长
 *  b) 统计每条路径中访问次数  
 *  key:  appId|platform|channel|gameServer
 *  value:{scenes}
 * @author mikefeng
 *
 */
public class SceneTreeSumMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {
	
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
		String UID = arr[i++];
		String loginTime = arr[i++];
		String scenes = arr[i++];
		
		String[] keyFields = new String[]{
				appId,
				platform,
				channel,
				gameServer
		};
		
		keyObj.setOutFields(keyFields);
		valObj.set(scenes);
		context.write(keyObj, valObj);
	}
}
