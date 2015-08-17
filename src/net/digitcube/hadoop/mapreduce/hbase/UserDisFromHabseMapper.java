package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.EnumConstants;
import net.digitcube.hadoop.common.Gender;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.NetType;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.jce.OnlineDay;
import net.digitcube.hadoop.jce.PlayerDeviceInfo;
import net.digitcube.hadoop.jce.PlayerInfoForHbase;
import net.digitcube.hadoop.jce.PlayerInfoMap;
import net.digitcube.hadoop.mapreduce.hbase.util.HbasePool;
import net.digitcube.hadoop.mapreduce.hbase.util.HbaseProxyClient;
import net.digitcube.protocol.JceInputStream;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserDisFromHabseMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private Text mapValueObj = new Text();
	HConnection connection = null;

	private int daytime = 0;
	private Set<String> dimTypeSet = new HashSet<String>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		connection = HbasePool.getConnection();

		String[] dimTypes = context.getConfiguration()
				.get("dc.webfrontjob.dimtypes").split("\\|");
		for (String dimType : dimTypes) {
			dimTypeSet.add(dimType);
		}

		// statTime
		String yyyyMMdd = context.getConfiguration().get(
				"dc.webfrontjob.statTime");
		Calendar cal = Calendar.getInstance();
		String yyyy = yyyyMMdd.substring(0, 4);
		String MM = yyyyMMdd.substring(4, 6);
		String dd = yyyyMMdd.substring(6);
		cal.set(Calendar.YEAR, Integer.parseInt(yyyy));
		cal.set(Calendar.MONTH, Integer.parseInt(MM) - 1); // 月份减一
		cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(dd));
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		daytime = (int) (cal.getTimeInMillis() / 1000);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] params = value.toString().split(MRConstants.SEPERATOR_IN);
		String appId = params[0];
		String platformType = params[1];
		String accountId = params[2];

		String hbaseKey = appId + "|" + platformType + "|" + accountId;
		Result result = HbaseProxyClient.getOneRecord(connection, "users",
				hbaseKey);
		if (result == null) {
			return;
		}
		byte[] data = result
				.getValue(Bytes.toBytes("info"), Bytes.toBytes("q"));
		if (data == null) {
			return;
		}

		// 区服，渠道，等级，日在线时长，日登陆次数，日付费金额，日付费次数分布统计

		PlayerInfoMap playerInfoMap = new PlayerInfoMap();
		JceInputStream inputStream = new JceInputStream(data);
		playerInfoMap.readFrom(inputStream);

		for (Entry<String, PlayerInfoForHbase> entry : playerInfoMap
				.getPlayerInfoMap().entrySet()) {
			if (MRConstants.ALL_GAMESERVER.equals(entry.getKey())) {
				continue;
			}
			List<OnlineDay> list = entry.getValue().getOnlineDayList();
			OnlineDay onlineDay = null;
			if (list != null) {
				for (OnlineDay od : list) {
					if (od.getOnlineDate() == daytime) {
						onlineDay = od;
					}
				}
			}
			if (onlineDay == null)
				continue;
			// 区服分布
			if (dimTypeSet.contains(Constants.DIMENSION_PAY_ON_GAMESERVER)) {
				mapKeyObj
						.setOutFields(new String[] { appId, platformType,
								Constants.DIMENSION_PAY_ON_GAMESERVER,
								entry.getKey() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 渠道分布
			if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_CHANNEL)) {
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_PLAYER_CHANNEL,
						entry.getValue().getChannel() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 等级
			if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_LEVEL)) {
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_PLAYER_LEVEL,
						entry.getValue().getLevel() + "" });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 日在线时长
			if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_ONLINETIME)) {
				mapKeyObj.setOutFields(new String[] {
						appId,
						platformType,
						Constants.DIMENSION_PLAYER_ONLINETIME,
						EnumConstants.getDayOnlineTimeRange(onlineDay
								.getOnlineTime()) + "" });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 日登陆次数
			if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_LoginTimes)) {
				mapKeyObj.setOutFields(new String[] {
						appId,
						platformType,
						Constants.DIMENSION_PLAYER_LoginTimes,
						EnumConstants.getDayLoginTimesRange(onlineDay
								.getLoginTimes()) + "" });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			if (onlineDay.getPayAmount() > 0) {
				// 日付费金额分布
				if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_PayAmount)) {
					mapKeyObj.setOutFields(new String[] {
							appId,
							platformType,
							Constants.DIMENSION_PLAYER_PayAmount,
							EnumConstants.getDayPayAmountRange(onlineDay
									.getPayAmount()) + "" });
					mapValueObj.set(accountId);
					context.write(mapKeyObj, mapValueObj);
				}
				// 日付费次数分布
				if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_PayTimes)) {
					mapKeyObj.setOutFields(new String[] {
							appId,
							platformType,
							Constants.DIMENSION_PLAYER_PayTimes,
							EnumConstants.getDayPayTimesRange(onlineDay
									.getPayTimes()) + "" });
					mapValueObj.set(accountId);
					context.write(mapKeyObj, mapValueObj);
				}
			}
		}
		// 设备信息
		result = HbaseProxyClient.getOneRecord(connection, "device", hbaseKey);
		if (result == null) {
			return;
		}
		NavigableMap<byte[], byte[]> deviceMap = result.getFamilyMap(Bytes
				.toBytes("info"));
		if (deviceMap == null) {
			return;
		}
		for (Entry<byte[], byte[]> entry : deviceMap.entrySet()) {
			inputStream = new JceInputStream(entry.getValue());
			PlayerDeviceInfo deviceInfo = new PlayerDeviceInfo();
			deviceInfo.readFrom(inputStream);
			// 省份
			if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_AREA)) {
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_PLAYER_AREA,
						deviceInfo.getProvince() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 国家
			if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_COUNTRY)) {
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_PLAYER_COUNTRY,
						deviceInfo.getCountry() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 运营商
			if (dimTypeSet.contains(Constants.DIMENSION_DEVICE_OPERATOR)) {
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_DEVICE_OPERATOR,
						deviceInfo.getCarrier() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 性别
			if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_GENDER)) {
				Gender gender = Gender.convert(deviceInfo.getGender());
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_PLAYER_GENDER,
						gender == null ? "未知" : gender.toString() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 年龄
			if (dimTypeSet.contains(Constants.DIMENSION_PLAYER_AGE)) {
				mapKeyObj
						.setOutFields(new String[] {
								appId,
								platformType,
								Constants.DIMENSION_PLAYER_AGE,
								EnumConstants.getRangeTop4Age(deviceInfo
										.getAge()) + "" });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 机型
			if (dimTypeSet.contains(Constants.DIMENSION_DEVICE_BRAND)) {
				mapKeyObj
						.setOutFields(new String[] { appId, platformType,
								Constants.DIMENSION_DEVICE_BRAND,
								deviceInfo.getBrand() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 分辨率
			if (dimTypeSet.contains(Constants.DIMENSION_DEVICE_RESOL)) {
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_DEVICE_RESOL,
						deviceInfo.getResolution() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 操作系统版本
			if (dimTypeSet.contains(Constants.DIMENSION_DEVICE_OS)) {
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_DEVICE_OS,
						deviceInfo.getOpersystem() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			// 网络类型
			if (dimTypeSet.contains(Constants.DIMENSION_DEVICE_NETTYPE)) {
				NetType netType = NetType.convert(deviceInfo.getNetType());
				mapKeyObj.setOutFields(new String[] { appId, platformType,
						Constants.DIMENSION_DEVICE_NETTYPE,
						netType == null ? "OTHER" : netType.toString() });
				mapValueObj.set(accountId);
				context.write(mapKeyObj, mapValueObj);
			}
			break;
		}

		// mapValueObj.setOutFields(valFields);
		// context.write(mapKeyObj, mapValueObj);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		HbasePool.close(null, connection);
	}

}
