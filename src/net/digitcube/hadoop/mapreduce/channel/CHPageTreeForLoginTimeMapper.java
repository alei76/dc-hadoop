package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.channel.EventLog2;
import net.digitcube.hadoop.model.channel.EventLog2.AttrKey;
import net.digitcube.hadoop.model.channel.HeaderLog2.ExtendKey;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输入：页面浏览自定义事件 _DESelf_Channel_PageNavigation
 * 主要逻辑： 对同一玩家同一次登录所浏览的页面按浏览时间排序并输出
 *
 * 输出:
 * 1.每个用户每次登录的详情
 * Key:				appId,appVersion,channel,country,province,uid,loginTime
 * Value			resumeTime,pageName,duration
 * 
 */
public class CHPageTreeForLoginTimeMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog2 eventLog;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog2(arr);
		} catch (Exception e) {
			return;
		}
		
		int duration = StringUtil.convertInt(eventLog.getDuration(), 0);
		String pageName = eventLog.getAttrMap().get(AttrKey.PAGE_NAME);
		String loginTime = eventLog.getAttrMap().get(AttrKey.LOGIN_TIME);
		//页面开始浏览时间
		String resumeTime = eventLog.getAttrMap().get(AttrKey.RESUME_TIME);
		if(StringUtil.isEmpty(pageName) || StringUtil.isEmpty(loginTime)
				|| StringUtil.isEmpty(resumeTime)){
			return;
		}
		
		String[] keyFields = new String[]{
				eventLog.getAppID(),
				eventLog.getAppVersion(),
				eventLog.getChannel(),
				eventLog.getExtendValue(ExtendKey.CNTY),
				eventLog.getExtendValue(ExtendKey.PROV),
				eventLog.getUID(),
				loginTime
		};
		String[] valFields = new String[]{
				resumeTime,
				pageName,
				duration + ""
		};
		
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);

	}
}
