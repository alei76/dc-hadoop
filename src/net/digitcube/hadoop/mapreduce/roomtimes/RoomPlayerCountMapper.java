package net.digitcube.hadoop.mapreduce.roomtimes;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * 输入:
 * RoomAndUserRollingReducer中输出文件:
 * appID|platform|accountID|channel|gameServer|accountType|gender|age
 * |province|roomid|newaddcount|onlinecount|paymentcount
 * 后缀为ROOM_DATA
 * 输出:
 * key:appID|platform|channel|gameServer|type|userType|roomId
 * value:1
 * @author mikefeng
 *
 */
public class RoomPlayerCountMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {	
	
	private OutFieldsBaseModel outputKey = new OutFieldsBaseModel();
	private IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		if (fileSuffix.contains(Constants.SUFFIX_ROOM_DATA)) {
			String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
			String type = Constants.DIMENSION_ROOM_USERTYPE_ACCOUNT_SUM;
			int newAddCount = StringUtil.convertInt(arr[10], 0);
			int onlineCount = StringUtil.convertInt(arr[11], 0);
			int paymentCount = StringUtil.convertInt(arr[12], 0);
			String[] outputKeyArr = null;
			//key : appID|platform|channel|gameServer|type|userType|roomId
			outputKey.setSuffix(Constants.SUFFIX_ROOM_USER_ACCOUNT);
			if( newAddCount > 0){
				 outputKeyArr = new String[] {arr[0], arr[1], arr[3],arr[4],type,Constants.PLAYER_TYPE_NEWADD,arr[9]};
				 outputKey.setOutFields(outputKeyArr);			
				 context.write(outputKey, one);
			} 
			if( onlineCount > 0){
				 outputKeyArr = new String[] {arr[0], arr[1], arr[3],arr[4],type,Constants.PLAYER_TYPE_ONLINE,arr[9]};
				 outputKey.setOutFields(outputKeyArr);			
				 context.write(outputKey, one);
			}
			if( paymentCount > 0){
				 outputKeyArr = new String[] {arr[0], arr[1], arr[3],arr[4],type,Constants.PLAYER_TYPE_PAYMENT,arr[9]};
				 outputKey.setOutFields(outputKeyArr);			
				 context.write(outputKey, one);
			}			
		}
	}
}
