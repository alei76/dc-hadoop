package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.util.Date;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 临时用（只用一次），主要逻辑
 * 
 * 把滚存中所有玩家的新增日期、首付日期入到 HBase 表中
 */

public class PlayerNewAddInfoHBaseRollingMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	//private byte[] info = NewAddPlayerInHBaseMapper.info;
	//private byte[] fstLgDay = NewAddPlayerInHBaseMapper.fstLgDay;//firstLoginDay
	//private byte[] fstPayDay = NewAddPlayerInHBaseMapper.fstPayDay;//firstPayDay
	private ImmutableBytesWritable table = new ImmutableBytesWritable(PlayerNewAddInfoHBaseMapper.TB_USER_NEWADD_INFO);

	private Date statDate;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = ConfigManager.getInitialDate(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoRollingLog userInfoRollingLog = new UserInfoRollingLog(statDate, array);
		String pureAppId = userInfoRollingLog.getAppID().split("\\|")[0];
		String platform = userInfoRollingLog.getPlatform();
		String gameServer = userInfoRollingLog.getPlayerDayInfo().getGameRegion();
		String accountId = userInfoRollingLog.getAccountID();
		int firstLoginDay = userInfoRollingLog.getPlayerDayInfo().getFirstLoginDate();
		int firstPayDay = userInfoRollingLog.getPlayerDayInfo().getFirstPayDate();
		
		String rowKey = pureAppId + "|" + platform + "|" + gameServer + "|" + accountId;
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(PlayerNewAddInfoHBaseMapper.info, PlayerNewAddInfoHBaseMapper.fstLgDay, Bytes.toBytes(firstLoginDay));
		if(firstPayDay > 0){
			put.add(PlayerNewAddInfoHBaseMapper.info, PlayerNewAddInfoHBaseMapper.fstPayDay, Bytes.toBytes(firstPayDay));
		}
		
		//write to table
		context.write(table, put);
	}
}
