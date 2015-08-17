package net.digitcube.hadoop.mapreduce.channel.event;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 新版事件日志分离Mapper
 * 对不同的自定义事件根据ID分离
 * 并分别输出到以事件ID为后缀的文件中
 * 
 * @author sam.xie
 * @date 2015年3月5日 下午4:41:00
 * @version 1.0
 */
public class CHEventSeparatorMapper extends Mapper<LongWritable, Text, Text, OutFieldsBaseModel> {

	private Text keyObj = new Text();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 某些字段中存在换行符，导致计算出错，需去掉
		String line = value.toString().replace("\r", "").replace("\n", "");
		String[] array = line.split(MRConstants.SEPERATOR_IN);
		EventLog2 eventLog = null;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整
			// 这里只解析新版的事件
			eventLog = new EventLog2(array);
		} catch (Exception e) {
			// TODO do something to mark the error here
			return;
		}
		
		String eventID = eventLog.getEventId();
		eventID = eventID.contains(":") ? eventID.split(":")[0] : eventID;
		eventID = eventID.startsWith("_") ? eventID.replaceFirst("_", "") : eventID;

		keyObj.set(eventID);
		// keyObj.setSuffix(eventID);
		// keyObj.setOutFields(array);

		// Added at 20140606 : iOS 渠道修正
		// 20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
		// if(MRConstants.PLATFORM_iOS_STR.equals(eventLog.getPlatform())){
		/*String reviseChannel = IOSChannelUtil.checkForiOSChannel(eventLog.getAppID(), eventLog.getUID(),
				eventLog.getPlatform(), eventLog.getChannel());
		eventLog.setChannel(reviseChannel);*/
		// }

		valObj.setOutFields(array);

		/*// 临时兼容 html5 的事件
		if (filePath.contains("logserver-js")) {
			valObj.setOutFields(array);
		}*/

		context.write(keyObj, valObj);
	}
}
