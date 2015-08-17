package net.digitcube.hadoop.mapreduce.plugin;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.JdbcUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.hsqldb.lib.StringUtil;

/**
 * 
 * <pre>
 * 虚拟币的获得与消耗流水
 * 读取原始的EventSelf日志
 * @author Ivan          <br>
 * @date 2015年1月20日 上午11:25:45 <br>
 * @version 1.0
 * <br>
 */
public class CoinReportMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	private Calendar calendar = null;
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel valueObj = new OutFieldsBaseModel();
	private final static int pluginType = 2;
	private Set<String> filterSet = new HashSet<String>();

	private String fileSuffix;
	public final static String Event_Coin_Lost = "_DESelf_Coin_Lost";
	public final static String Event_Coin_Gain = "_DESelf_Coin_Gain";

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		filterSet = JdbcUtil.getPluginConfig(pluginType);
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 如果没有任何pluginid=2的配置就不继续处理
		if (filterSet.size() <= 0) {
			return;
		}
		// 读取一行记录进来
		// 消费日期，渠道，区服，设备ID，账号ID，mac，imei，idfa，类型，产出/消耗，原因，数量
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整 用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog(paraArr);
		} catch (Exception e) {
			// TODO do something to mark the error here
			return;
		}
		if (!FieldValidationUtil.validateAppIdLength(eventLog.getAppID())) {
			return;
		}
		// 获取appID和version,如果appID不需要过滤就不处理
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

		String coinType = eventLog.getArrtMap().get("coinType");
		coinType = null == coinType ? Constants.DEFAULT_COIN_TYPE : coinType;

		String reason = eventLog.getArrtMap().get("id");
		reason = StringUtil.isEmpty(reason) ? "-" : reason;
		String num = eventLog.getArrtMap().get("num");
		if (StringUtil.isEmpty(num)) {
			return;
		}
		// 消费日期，，mac，imei，idfa，类型，产出/消耗，原因，数量
		keyObj.setOutFields(new String[] { appId, platform, appVersion, channel, gameServer, uid, accountId, mac, imei,
				idfa, Event_Coin_Lost.equals(eventLog.getEventId()) ? "0" : "1", coinType });
		valueObj.setOutFields(new String[] { reason, num });
		keyObj.setSuffix(Constants.SUFFIX_PLUGIN_CoinReport);
		context.write(keyObj, valueObj);
	}
}
