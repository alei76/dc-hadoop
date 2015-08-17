package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 输入：
 * a) 最后使用页面和功能滚存
 * b) 滚存中输出的流失用户 
 * c) 自定义事件 DESelf_APP_NAVIGATION 和 DESelf_APP_MODULE
 * 
 * 主要逻辑： 
 * a) 统计每个页面和每个功能的访问次数和时长
 * b) 把每个玩家最后使用的功能和页面更新到滚存
 * c) 输出每个流失玩家的最后使用功能和页面
 * 
 * 输出：
 * a) 滚存： 
 * 		appId, platform, channel, gameServer, accountId, lastViewPage, lastViewPageTime, lastUseModule, lastUseModuleTime
 * b) 流失用户最后使用页面和功能：
 * 		appId, platform, channel, gameServer, accountId, lostType(14/30), lastViewPage, lastUseModule
 * c) 页面和功能使用次数和时长：
 * 		appId, platform, channel, gameServer, type(page|module), name(pageName|moduleName), valType(totalViewTimes|totalDuration), value
 */
public class PageAndModuleRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	static final String DATA_FLAG_ROLL = "R";
	static final String DATA_FLAG_USERLOST = "L";
	static final String DATA_FLAG_PAGE = "P";
	static final String DATA_FLAG_MODULE = "M";
	
	//20140627:与高境/TCL 约定，module 和  page 的 duration 超过  2 小时则丢弃
	private static final int MAX_DURATIONR = 2 * 3600;
	
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	private String fileName = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		
		int i = 0;
		if(fileName.endsWith(Constants.SUFFIX_APP_PAGE_MODULE_ROLL)){
			
			//验证appId长度 并修正appId  add by mikefeng 20141010
			if(!FieldValidationUtil.validateAppIdLength(arr[0])){
				return;
			}
			
			String appId = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String gameServer = arr[i++];
			String accountId = arr[i++];
			String lastViewPage = arr[i++];
			String lastViewPageTime = arr[i++];
			String lastUseModule = arr[i++];
			String lastUseModuleTime = arr[i++];
			
			String[] keyFields = new String[]{
					appId,
					platform,
					channel,
					gameServer,
					accountId
			};
			String[] valFields = new String[]{
					lastViewPage,
					lastViewPageTime,
					lastUseModule,
					lastUseModuleTime
			};
			keyObj.setSuffix("");
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_ROLL);
			context.write(keyObj, valObj);
			
		}else if(fileName.endsWith(Constants.SUFFIX_USERFLOW)){			
			String appId = arr[i++];
			String platform = arr[i++];
			String channel = arr[i++];
			String gameServer = arr[i++];
			String userFlowType = arr[i++];
			String level = arr[i++];
			String accountId = arr[i++];

			//只统计 14/30 日流失玩家
			if(Constants.UserLostType.UserLost14.value.equals(userFlowType)
					|| Constants.UserLostType.UserLost30.value.equals(userFlowType)){
				
				String[] keyFields = new String[]{
						appId,
						platform,
						channel,
						gameServer,
						accountId
				};
				
				String[] valFields = new String[]{
						userFlowType
				};
				keyObj.setSuffix("");
				keyObj.setOutFields(keyFields);
				valObj.setOutFields(valFields);
				valObj.setSuffix(DATA_FLAG_USERLOST);
				context.write(keyObj, valObj);
			}
			
		}else if(fileName.endsWith(Constants.DESelf_APP_NAVIGATION)){
			EventLog event = new EventLog(arr);	
			
			int duration = event.getDuration();
			
			//20140627:与高境/TCL 约定，module 和  page 的 duration 超过  2 小时则丢弃
			if(duration > MAX_DURATIONR){
				return;
			}
			
			String pageName = event.getArrtMap().get("pageName");
			String loginTime = event.getArrtMap().get("loginTime");
			String resumeTime = event.getArrtMap().get("resumeTime");
			if(StringUtil.isEmpty(pageName) || StringUtil.isEmpty(loginTime)
					|| StringUtil.isEmpty(resumeTime)){
				return;
			}
			
			String appId = event.getAppID();
			String platform = event.getPlatform();
			String channel = event.getChannel();
			String gameServer = event.getGameServer();
			String accountId = event.getAccountID();
			String uid = event.getUID();
			
			//最后使用页面统计-------------------------------------
			String[] keyFields = new String[]{
					appId,
					platform,
					channel,
					gameServer,
					accountId
			};
			String[] valFields = new String[]{
					pageName,
					resumeTime
			};
			keyObj.setSuffix("");
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_PAGE);
			context.write(keyObj, valObj);
			
			//全服
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			keyObj.setSuffix("");
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_PAGE);
			context.write(keyObj, valObj);
			
			//页面访问时长及次数统计-------------------------------------
			String[] keyFields1 = new String[]{
					appId,
					platform,
					channel,
					gameServer,
					Constants.DIMENSION_APP_PAGE,//dimenType
					pageName,//vkey1
			};
			String[] valFields1 = new String[]{
					duration + "",
					uid
			};
			keyObj.setOutFields(keyFields1);
			valObj.setOutFields(valFields1);
			keyObj.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_TIMES_DUR);
			context.write(keyObj, valObj);
			//全服
			keyFields1[3] = MRConstants.ALL_GAMESERVER;
			keyObj.setOutFields(keyFields1);
			valObj.setOutFields(valFields1);
			keyObj.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_TIMES_DUR);
			context.write(keyObj, valObj);
			
		}else if(fileName.endsWith(Constants.DESelf_APP_MODULE)){
			EventLog event = new EventLog(arr);
			
			int duration = event.getDuration();
			//20140627:与高境/TCL 约定，module 和  page 的 duration 超过  2 小时则丢弃
			if(duration > MAX_DURATIONR){
				return;
			}
			
			String moduleName = event.getArrtMap().get("moduleName");
			//自定义事件里名字拼写错误兼容
			if(StringUtil.isEmpty(moduleName)){
				moduleName = event.getArrtMap().get("modeluName");
			}
			String resumeTime = event.getArrtMap().get("resumeTime");
			if(StringUtil.isEmpty(moduleName) || StringUtil.isEmpty(resumeTime)){
				return;
			}
			
			String appId = event.getAppID();
			String platform = event.getPlatform();
			String channel = event.getChannel();
			String gameServer = event.getGameServer();
			String accountId = event.getAccountID();
			String uid = event.getUID();
			
			//最后使用功能访统计-------------------------------------
			String[] keyFields = new String[]{
					appId,
					platform,
					channel,
					gameServer,
					accountId
			};
			String[] valFields = new String[]{
					moduleName,
					resumeTime
			};
			keyObj.setSuffix("");
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_MODULE);
			context.write(keyObj, valObj);
			//全服
			keyFields[3] = MRConstants.ALL_GAMESERVER;
			keyObj.setSuffix("");
			keyObj.setOutFields(keyFields);
			valObj.setOutFields(valFields);
			valObj.setSuffix(DATA_FLAG_MODULE);
			context.write(keyObj, valObj);
			
			//功能访问时长及次数统计-------------------------------------
			String[] keyFields1 = new String[]{
					appId,
					platform,
					channel,
					gameServer,
					Constants.DIMENSION_APP_FUNCTION,//dimenType
					moduleName,//vkey1
			};
			String[] valFields1 = new String[]{
					duration + "",
					uid
			};
			keyObj.setOutFields(keyFields1);
			valObj.setOutFields(valFields1);
			keyObj.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_TIMES_DUR);
			context.write(keyObj, valObj);
			//全服
			keyFields1[3] = MRConstants.ALL_GAMESERVER;
			keyObj.setOutFields(keyFields1);
			valObj.setOutFields(valFields1);
			keyObj.setSuffix(Constants.SUFFIX_APP_PAGE_MODULE_TIMES_DUR);
			context.write(keyObj, valObj);
		}
	}
}
