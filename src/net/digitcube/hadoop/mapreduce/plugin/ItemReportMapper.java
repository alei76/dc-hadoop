package net.digitcube.hadoop.mapreduce.plugin;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.JdbcUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ItemReportMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private Calendar calendar = null;
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valueObj = new OutFieldsBaseModel();
	private final static int pluginType = 3;
	private Set<String> filterSet = new HashSet<String>();

	private String fileSuffix;
	public final static String Event_ItemBuy = "_DESelf_ItemBuy";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		filterSet = JdbcUtil.getPluginConfig(pluginType);
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (filterSet.size() <= 0) {
			return;
		}

		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog(paraArr);
		} catch (Exception e) {
			// TODO do something to mark the error here
			return;
		}
		if (!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())) {
			return;
		}

		String[] appInfo = eventLog.getAppID().split("\\|");
		String appId = appInfo[0];
		String appVersion = appInfo[1];
		if (!filterSet.contains(appId)) {
			return;
		}
		String platform = eventLog.getPlatform();
		String uid = eventLog.getUID();
		String mac = eventLog.getMac();
		mac = StringUtil.isEmpty(mac) ? "-" : mac;
		String imei = eventLog.getImei();
		imei = StringUtil.isEmpty(imei) ? "-" : imei;
		String idfa = eventLog.getIdfa();
		idfa = StringUtil.isEmpty(idfa) ? "-" : idfa;
		String accountId = eventLog.getAccountID();
		String channel = eventLog.getChannel();
		String gameServer = eventLog.getGameServer();

		// 道具 道具类型 币种 虚拟币数量 购入数量
		Map<String, String> map = eventLog.getArrtMap();
		String itemId = map.get("itemId");// 道具
		String itemType = map.get("itemType");// 道具类型
		itemType = StringUtil.isEmpty(itemType) ? "none" : itemType;
		String currencyType = map.get("currencyType");// 币种
		String itemCnt = map.get("itemCnt");// 购入数量
		String virtualCurrency = map.get("virtualCurrency");// 虚拟币数量
		if (null == virtualCurrency) {
			virtualCurrency = map.get("vituralCurrency");
		}

		if (StringUtil.isEmpty(itemId) || StringUtil.isEmpty(itemType) || StringUtil.isEmpty(itemCnt)
				|| StringUtil.isEmpty(currencyType) || StringUtil.isEmpty(virtualCurrency)) {
			return;
		}
		// 消费日期，渠道，区服，设备ID，账号ID，mac，imei，idfa，道具 道具类型 币种 虚拟币数量 购入数量
		keyObj.setOutFields(new String[] { appId, platform, appVersion, channel, gameServer, uid, accountId, mac, imei,
				idfa, itemId, itemType });
		valueObj.setOutFields(new String[] { currencyType, virtualCurrency, itemCnt });
		keyObj.setSuffix(Constants.SUFFIX_PLUGIN_ItemReport);
		context.write(keyObj, valueObj);
	}
}
