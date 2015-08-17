package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.util.DateUtil;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 把滚存中当天活跃玩家的变更信息更新到  HBase 表中:
 * 首登日期/末登日期/总在线天数/总在线时长/等级
 * 首付日期/末付日期/总付费次数/总付费金额
 */

public class PlayerBasicInfo4HBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	private ImmutableBytesWritable table = new ImmutableBytesWritable(PlayerBasicInfo.TB_PLAYER_BASIC_INFO);

	private Date statDate;
	// 数据时间
	private int statTime;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = ConfigManager.getInitialDate(context.getConfiguration());
		statTime = DateUtil.getStatDate(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoRollingLog userInfo = new UserInfoRollingLog(statDate, array);
		String pureAppId = userInfo.getAppID().split("\\|")[0];
		String platform = userInfo.getPlatform();
		String gameServer = userInfo.getPlayerDayInfo().getGameRegion();
		String accountId = userInfo.getAccountID();
		
		String rowKey = pureAppId + "|" + platform + "|" + gameServer + "|" + accountId;
		Put put = new Put(Bytes.toBytes(rowKey));
		
		// 首登日期
		if(statTime == userInfo.getPlayerDayInfo().getFirstLoginDate()){
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.fstLgDay, Bytes.toBytes(statTime));
		}
		// 当天有登录
		if(userInfo.isLogin(statTime, statTime)){
			//末登日期
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.lstLgDay, Bytes.toBytes(statTime));
			//玩家等级
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.level, Bytes.toBytes(userInfo.getPlayerDayInfo().getLevel()));
			//总在线天数
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.totOlDay, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalOnlineDay()));
			//总在线天数
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.totOlTim, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalOnlineTime()));
			//总在线天数
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.totLgTms, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalLoginTimes()));
		}
		
		// 首付日期
		if(statTime == userInfo.getPlayerDayInfo().getFirstPayDate()){
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.fstPayDay, Bytes.toBytes(statTime));
		}
		// 当天有付费
		if(userInfo.isPay(statTime, statTime)){
			//末付日期
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.lstPayDay, Bytes.toBytes(statTime));
			//总付费金额
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.totPayAmt, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalCurrencyAmount()));
			//总付费次数
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.totPayTms, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalPayTimes()));
		}
		
		//write to table
		context.write(table, put);
	}
}
