package net.digitcube.hadoop.mapreduce.plugin;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.PaymentLog;
import net.digitcube.hadoop.util.FieldValidationUtil;
import net.digitcube.hadoop.util.JdbcUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PaymentReportPluginMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {
	private Calendar calendar = null;
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private final static int pluginType = 1;
	private Set<String> filterSet = new HashSet<String>();
	private String fileSuffix;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		filterSet = JdbcUtil.getPluginConfig(1);
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paymentArr = value.toString().split(MRConstants.SEPERATOR_IN);
		PaymentLog paymentLog = null;
		try {
			// 日志里某些字段有换行字符，导致这条日志换行而不完整，
			// 用 try-catch 过滤掉这类型的错误日志
			paymentLog = new PaymentLog(paymentArr);
		} catch (Exception e) {
			// TODO do something to mark the error here
			return;
		}

		// 验证appId长度 并修正appId add by mikefeng 20141010
		if (!FieldValidationUtil.validateAppIdLength(paymentLog.getAppID())) {
			return;
		}

		String[] appInfo = paymentLog.getAppID().split("\\|");
		String appId = appInfo[0];
		String appVersion = appInfo[1];
		if (!filterSet.contains(appId)) {
			return;
		}
		String platform = paymentLog.getPlatform();
		String uid = paymentLog.getUID();
		String mac = paymentLog.getMac();
		mac = StringUtil.isEmpty(mac) ? "-" : mac;
		String imei = paymentLog.getImei();
		imei = StringUtil.isEmpty(imei) ? "-" : imei;
		String idfa = paymentLog.getIdfa();
		idfa = StringUtil.isEmpty(idfa) ? "-" : idfa;
		String roleID = paymentLog.getRoleID();
		roleID = StringUtil.isEmpty(roleID) ? "-" : roleID;
		String roleName = paymentLog.getRoleName();
		roleName = StringUtil.isEmpty(roleName) ? "-" : roleName;

		String accountId = paymentLog.getAccountID();
		String channel = paymentLog.getChannel();
		String gameServer = paymentLog.getGameServer();
		float currencyAmount = paymentLog.getCurrencyAmount();
		String currencyType = paymentLog.getCurrencyType();
		String iapid = paymentLog.getIapid();
		int payTime = paymentLog.getPayTime();
		String payType = paymentLog.getPayType();
		String orderId = paymentLog.getOrderId();
		currencyType = StringUtil.isEmpty(currencyType) ? "-" : currencyType;
		payType = StringUtil.isEmpty(payType) ? "-" : payType;
		orderId = StringUtil.isEmpty(orderId) ? "-" : orderId;
		iapid = StringUtil.isEmpty(iapid) ? "-" : iapid;
		keyObj.setOutFields(new String[] { appId, platform, appVersion, channel, gameServer, orderId, uid, accountId,
				mac, imei, idfa, currencyAmount + "", payTime + "", currencyType, payType, iapid, roleID, roleName });
		keyObj.setSuffix(Constants.SUFFIX_PLUGIN_PaymentReport);
		context.write(keyObj, NullWritable.get());
	}
}
