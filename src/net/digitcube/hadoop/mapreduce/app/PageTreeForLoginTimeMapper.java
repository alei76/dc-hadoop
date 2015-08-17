package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输入：页面浏览自定义事件 DESelf_APP_NAVIGATION
 * 主要逻辑： 对同一玩家同一次登录所浏览的页面按浏览时间排序并输出
 *
 * Reduce 输出:
 * appId, platform, channel, gameServer, accoundId, loginTime, [page1:dur,page2:dur,...,pageN:dur]
 */
public class PageTreeForLoginTimeMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog event = new EventLog(arr);
		int duration = event.getDuration();
		String pageName = event.getArrtMap().get("pageName");
		String loginTime = event.getArrtMap().get("loginTime");
		//页面开始浏览时间
		String resumeTime = event.getArrtMap().get("resumeTime");
		if(StringUtil.isEmpty(pageName) || StringUtil.isEmpty(loginTime)
				|| StringUtil.isEmpty(resumeTime)){
			return;
		}
		
		String[] keyFields = new String[]{
				event.getAppID(),
				event.getPlatform(),
				event.getChannel(),
				event.getGameServer(),
				event.getAccountID(),
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
		
		//全服
		keyFields[3] = MRConstants.ALL_GAMESERVER;
		keyObj.setOutFields(keyFields);
		valObj.setOutFields(valFields);
		context.write(keyObj, valObj);
	}
}
