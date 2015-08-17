package net.digitcube.hadoop.tmp;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class XiyangyangFilterMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	String appIdFilter = "E9C7C104F71C470587D6120CC1415146";
	String accountIdFilter = "B851E38DA32B30D47C86A23BB4F1B2C5:50CD0FE336CCB97BC0307C01A1E1D29A:FF5481F08562D4D140712E608372A6AB:52ED8AB5F630DFBF091A0186DC6CD5F4:FA04AB25FE699CEF05D378758110B106:E86942BC9C9CC1891BB9637E182C63C5:73379F8B3C13B71B8C0ABF60BDAF46A7:50CD0FE336CCB97BC0307C01A1E1D29A:1893C1DBA0323853B11543CF8BEBCEDF:92D7DF65EACF335B1A472F54588A24E7:E86942BC9C9CC1891BB9637E182C63C5:95618A5791E161771724A07C2D037E8A:1893C1DBA0323853B11543CF8BEBCEDF:E86942BC9C9CC1891BB9637E182C63C5:E86942BC9C9CC1891BB9637E182C63C5:E86942BC9C9CC1891BB9637E182C63C5:45E4F7A6A4D9AF1FA26ADCD30187E2D4:85F812F052C9DC1047CFF22E719501EF:E483DD5B8CB42C8391B78670297B927D:B851E38DA32B30D47C86A23BB4F1B2C5";
	String fileName = "";
	Set<String> accountSet = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		accountIdFilter = context.getConfiguration().get("accountIdFilter", accountIdFilter);
		appIdFilter = context.getConfiguration().get("appIdFilter", appIdFilter);
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		if (!"ALL".equals(accountIdFilter)) { // 如果accountIdFilter = ALL，表示不需要过滤accountId
			for (String id : accountIdFilter.split(":")) {
				accountSet.add(id);
				System.out.println("id -> " + id);
			}
		}
		System.out.println("accountSet -> " + accountSet);
		System.out.println("appIdFilter -> " + appIdFilter);
		System.out.println("fileName -> " + fileName);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try {
			eventLog = new EventLog(paraArr);
		} catch (Exception e) {
			return;
		}
		String appId = eventLog.getAppID().split("\\|")[0];
		String accountId = eventLog.getAccountID();
		String eventId = eventLog.getEventId();
		if (accountSet.size() > 0 && (!accountSet.contains(accountId))) {
			return;
		}
		if (!appIdFilter.equals(appId)) {
			return;
		}
		System.out.println("value -> " + value);
		// context.write(mapKeyObj, NullWritable.get());
		if (eventId.endsWith(Constants.DESelf_LevelsBegin)) {// 关卡开始
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix(Constants.DESelf_LevelsBegin);
			context.write(mapKeyObj, NullWritable.get());
		} else if (eventId.endsWith(Constants.DESelf_LevelsEnd)) { // 关卡结束
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix(Constants.DESelf_LevelsEnd);
			context.write(mapKeyObj, NullWritable.get());
		} else if (eventId.endsWith(Constants.DESelf_Coin_Gain)) { // 虚拟币获取
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix(Constants.DESelf_Coin_Gain);
			context.write(mapKeyObj, NullWritable.get());
		} else if (eventId.endsWith(Constants.DESelf_Coin_Lost)) { // 虚拟币消费
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix(Constants.DESelf_Coin_Lost);
			context.write(mapKeyObj, NullWritable.get());
		} else if (eventId.endsWith(Constants.DESelf_ItemBuy)) {// 道具购买
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix(Constants.DESelf_ItemBuy);
			context.write(mapKeyObj, NullWritable.get());
		} else if (eventId.endsWith(Constants.DESelf_ItemGet)) {// 道具获取
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix(Constants.DESelf_ItemGet);
			context.write(mapKeyObj, NullWritable.get());
		} else if (eventId.endsWith(Constants.DESelf_ItemUse)) {// 道具使用
			mapKeyObj.setOutFields(paraArr);
			mapKeyObj.setSuffix(Constants.DESelf_ItemUse);
			context.write(mapKeyObj, NullWritable.get());
		}
	}
}
