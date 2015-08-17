package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre/>
 * 关卡分析 & 关卡道具分析
 * 
 * 输入：
 * 24小时时间片的自定义事件日志
 * 首次冲关消耗时间：
 * 关卡首胜前失败次数：
 * 关卡道具使用：
 * 关卡通过星级统计：
 * 
 * 输出：
 * 首次成功通关时长：		版本号，关卡ID，首次通关人数，成功前尝试次数，第一次成功通关总耗时,
 * 关卡道具使用率：		版本号，关卡ID，道具ID，道具使用人数，道具使用次数
 * 关卡通过星级统计：		版本号，关卡ID，星级ID，星级人数
 * @author sam.xie
 * @date 2015年5月19日 上午11:23:31
 * @version 1.0
 */
public class GradeItemMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private String fileSuffix = "";

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	private Set<String> eventFilter = new HashSet<String>();
	private Set<String> appIDFilter = new HashSet<String>();

	private final String EVENT_GRADE_SUCC = "首次冲关消耗时间";
	private final String EVENT_GRADE_FAIL = "关卡首胜前失败次数";
	private final String EVENT_GRADE_ITEM = "关卡道具使用";
	private final String EVENT_GRADE_STAR = "关卡通过星级统计";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
		eventFilter.add(EVENT_GRADE_SUCC);
		eventFilter.add(EVENT_GRADE_FAIL);
		eventFilter.add(EVENT_GRADE_ITEM);
		eventFilter.add(EVENT_GRADE_STAR);
//		appIDFilter.add("56706B35787161602B8BA0A01FE5EF97");
		// 使用配置文件读取appID
		String appID = context.getConfiguration().get("dc.filter.appid");
		if(StringUtil.isEmpty(appID)){
			appID = "8B71BBCFF7E33EE2479BF8926F3B1A16";
			System.out.println("default.appid" + appID);
		}else{			
			System.out.println("dc.filter.appid=" + appID);
		}
		appID = appID.trim();
		appIDFilter.add(appID);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (arr.length < 23) {
			return;
		}
		EventLog event = new EventLog(arr);
		String accountID = event.getAccountID();
		String eventID = event.getEventId();
		String[] appIDAndVersion = event.getAppID().split("\\|");
		if(appIDAndVersion.length < 2){
			return;
		}
		String appID = appIDAndVersion[0];
		String version = appIDAndVersion[1];
		if (!(eventFilter.contains(eventID) && appIDFilter.contains(appID))) {
			return;
		}

		if (EVENT_GRADE_SUCC.equals(eventID)) {// 通过成功
			int duration = event.getDuration();
			String eventAttr = event.getArrtMap().get(EVENT_GRADE_SUCC); // 关卡2_首冲成功
			String grade = eventAttr.split("_")[0];
			keyObj.setSuffix("GRADE");
			keyObj.setOutFields(new String[] { appID, version, grade });
			valObj.setOutFields(new String[] { accountID, duration + "" });
			valObj.setSuffix("SUCC");
			context.write(keyObj, valObj);
		} else if (EVENT_GRADE_FAIL.equals(eventID)) { // 通关失败
			String eventAttr = event.getArrtMap().get(EVENT_GRADE_FAIL);
			String grade = eventAttr.split("_")[0];
			keyObj.setSuffix("GRADE");
			keyObj.setOutFields(new String[] { appID, version, grade });
			valObj.setOutFields(new String[] { accountID });
			valObj.setSuffix("FAIL");
			context.write(keyObj, valObj);
		} else if (EVENT_GRADE_ITEM.equals(eventID)) { // 关卡道具使用
			String eventAttr = event.getArrtMap().get(EVENT_GRADE_ITEM);
			String grade = eventAttr.split("_")[0];
			String item = eventAttr.split("_")[1];
			keyObj.setSuffix("ITEM");
			keyObj.setOutFields(new String[] { appID, version, grade, item });
			valObj.setOutFields(new String[] { accountID });
			context.write(keyObj, valObj);

			// 全部道具汇总
			keyObj.setOutFields(new String[] { appID, version, grade, "ALL_ITEM" });
			context.write(keyObj, valObj);
		} else if (EVENT_GRADE_STAR.equals(eventID)) { // 通关星级
			String eventAttr = event.getArrtMap().get(EVENT_GRADE_STAR);
			String grade = eventAttr.split("_")[0];
			String star = eventAttr.split("_")[1];
			keyObj.setSuffix("STAR");
			keyObj.setOutFields(new String[] { appID, version, grade, star });
			valObj.setOutFields(new String[] { accountID });
			context.write(keyObj, valObj);

			// 全部星级汇总
			keyObj.setOutFields(new String[] { appID, version, grade, "ALL_STAR" });
			context.write(keyObj, valObj);
		}
	}
}
