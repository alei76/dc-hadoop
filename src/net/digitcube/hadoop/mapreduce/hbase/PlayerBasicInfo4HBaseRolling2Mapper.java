package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 把滚存所有玩家的信息更新到  HBase 表中: 一周或一月执行一次
 * 首登日期/末登日期/总在线天数/总在线时长/等级
 * 首付日期/末付日期/总付费次数/总付费金额
 */

public class PlayerBasicInfo4HBaseRolling2Mapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	private Date statDate;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = new Date();
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
		byte[] row = Bytes.toBytes(rowKey);
		ImmutableBytesWritable rowWritable = new ImmutableBytesWritable(row);
		
		Put put = new Put(row);
		
		//玩家等级
		put.add(PlayerBasicInfo.info, PlayerBasicInfo.level, Bytes.toBytes(userInfo.getPlayerDayInfo().getLevel()));
		//首登日期
		put.add(PlayerBasicInfo.info, PlayerBasicInfo.fstLgDay, Bytes.toBytes(userInfo.getPlayerDayInfo().getFirstLoginDate()));
		//末登日期
		put.add(PlayerBasicInfo.info, PlayerBasicInfo.lstLgDay, Bytes.toBytes(userInfo.getPlayerDayInfo().getLastLoginDate()));
		//总在线天数
		put.add(PlayerBasicInfo.info, PlayerBasicInfo.totOlDay, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalOnlineDay()));
		//总在线天数
		put.add(PlayerBasicInfo.info, PlayerBasicInfo.totOlTim, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalOnlineTime()));
		//总在线天数
		put.add(PlayerBasicInfo.info, PlayerBasicInfo.totLgTms, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalLoginTimes()));
		
		//付费信息
		if(userInfo.getPlayerDayInfo().getTotalCurrencyAmount() > 0){
			//首付日期
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.fstPayDay, Bytes.toBytes(userInfo.getPlayerDayInfo().getFirstPayDate()));
			//末付日期
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.lstPayDay, Bytes.toBytes(userInfo.getPlayerDayInfo().getLastPayDate()));
			//总付费金额
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.totPayAmt, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalCurrencyAmount()));
			//总付费次数
			put.add(PlayerBasicInfo.info, PlayerBasicInfo.totPayTms, Bytes.toBytes(userInfo.getPlayerDayInfo().getTotalPayTimes()));
		}
		
		//write to table
		context.write(rowWritable, put);
	}
}
