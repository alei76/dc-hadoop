package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.CommonExtend;
import net.digitcube.hadoop.mapreduce.domain.CommonHeader;
import net.digitcube.hadoop.mapreduce.domain.OnlineLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午3:43:17 @copyrigt www.digitcube.net
 * 
 *          输入：用户每小时在线日志<br/>
 *          1373952308 4024BB8DB0C7AABB987C5622BA8FD544
 *          90a7efd4e874a3ce5e7e8f55f3f023d98537062f 10001 IOS demo 2 1 0 -
 *          640*1136 iPhone Simulator Apple 2 中国 广东省 u 1373952355 30 1 <br/>
 *          输出：<br/>
 *          key : appid,platform,accountid,logintime<br/>
 *          value:channel,accounttype,gender,age,gameserver,resolution,
 *          opersystem,brand,nettype,country,province,operators,onlinetime,level
 */

/**
 * use @PalyerOnlineHourMapper and @PalyerOnlineHourReducer instead
 */
@Deprecated
public class UserLoginHourMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] onlineArr = value.toString().split(MRConstants.SEPERATOR_IN);

		CommonHeader header = new CommonHeader(onlineArr);
		CommonExtend extend = new CommonExtend(onlineArr);
		OnlineLog onlineLog = new OnlineLog(onlineArr);
		String[] keyFields = new String[] { header.getAppID(),
				header.getPlatform(), header.getAccountID(),
				onlineLog.getLoginTime() + "" };

		mapKeyObj.setOutFields(keyFields);

		String[] valueFields = new String[] { extend.getChannel(),
				extend.getAccountType(), extend.getGender(), extend.getAge(),
				extend.getGameServer(), extend.getResolution(),
				extend.getOperSystem(), extend.getBrand(), extend.getNetType(),
				extend.getCountry(), extend.getProvince(),
				extend.getOperators(), onlineLog.getOnlineTime() + "",
				onlineLog.getLevel() + "", };
		mapValueObj.setOutFields(valueFields);

		context.write(mapKeyObj, mapValueObj);
	}
}
