package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.AppModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.OnlineAndPayRolling;
import net.digitcube.hadoop.util.JdbcUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：Uid的应用偏好（最偏好的游戏类型）
 * update by sam 2015-08-04 15:28:46
 * UID + APP滚存新增platform, channel，导致日志解析修改
 * 
 * 
 * 输入：
 * 1.UID + APP 滚存	@see WHUidRollingMapper 输出 {@link Constants#SUFFIX_WAREHOUSE_UIDAPP_ROLLING}
 * uid	appId	platform	channel
 * lastUpdateTime
 * firstLoginTime	lastLoginTime	totalLoginTimes	totalDuration	totalLoginDays	 
 * firstPayTime	lastPayTime	totalPayTimes	totalPayAmount	totalPayDays
 * 30LoginMark	30LoginList[loginTime:duration,...,loginTime:duration] // 最近30天在线
 * 30PayMark	30PayList[payTime:payAmount,...,payTime:payAmount] //最近30天付费
 * 
 * 输出：
 * 1.最近30天玩过的不同游戏类型统计
 * uid	appType	lastUpdateTime	firstLoginTime	lastLoginTime	loginTimes	duration	loginDays	firstPayTime	lastPayTime	payTimes	payAmount	payDays
 * 
 * @author sam.xie
 * @date 2015年7月20日 下午2:23:33
 * @version 1.0
 */
public class WHUidAppHabitDayMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private Map<String, AppModel> appMap = new HashMap<String, AppModel>();
	private Date scheduleTime = null;
	private int statTime = 0;

	@Override
	protected void setup(Context context) {
		appMap = JdbcUtil.getAppInfoMap();
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		statTime = (int) (calendar.getTimeInMillis() / 1000);
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (paramsArr.length < 17) {
			return;
		}

		OnlineAndPayRolling rollingStat = new OnlineAndPayRolling(4, paramsArr);
		String uid = rollingStat.getKeys()[0];
		String appId = rollingStat.getKeys()[1];
		String lastUpdateTime = rollingStat.getLastLoginTime() + "";
		String firstLoginTime = rollingStat.getFirstLoginTime() + "";
		String lastLoginTime = rollingStat.getLastLoginTime() + "";
		String firstPayTime = rollingStat.getFirstPayTime() + "";
		String lastPayTime = rollingStat.getLastPayTime() + "";
		String loginMark = rollingStat.getLoginMark() + "";
		String loginRecord = rollingStat.getLoginRecord();
		String payMark = rollingStat.getPayMark() + "";
		String payRecord = rollingStat.getPayRecord();
		int loginTimes = 0;
		int duration = 0;
		int payTimes = 0;
		float payAmount = 0;
		if ((!StringUtil.isEmpty(loginRecord)) && (!"-".equals(loginRecord))) {
			String[] recordArr = loginRecord.split(",");
			for (String record : recordArr) {
				loginTimes += StringUtil.convertInt(record.split(":")[1], 0);
				duration += StringUtil.convertInt(record.split(":")[2], 0);
			}
		}
		if ((!StringUtil.isEmpty(payRecord)) && (!"-".equals(payRecord))) {
			String[] recordArr = payRecord.split(",");
			for (String record : recordArr) {
				payTimes += StringUtil.convertInt(record.split(":")[1], 0);
				payAmount += StringUtil.convertFloat(record.split(":")[2], 0);
			}
		}

		String appType = "UNKNOWN";
		AppModel appInfo = appMap.get(appId);
		if (null != appInfo && !StringUtil.isEmpty(appInfo.getTypeName())) {
			appType = appInfo.getTypeName();
		}

		// 只输出当天有在线或付费的记录
		if (StringUtil.convertInt(lastUpdateTime, 0) != statTime) {
			return;
		}

		keyFields.setOutFields(new String[] { uid, appType });
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_UID_APPHABIT);
		valFields.setOutFields(new String[] { lastUpdateTime, firstLoginTime, lastLoginTime, loginTimes + "",
				duration + "", loginMark, firstPayTime, lastPayTime, payTimes + "", payAmount + "", payMark });
		context.write(keyFields, valFields);
	}
}
