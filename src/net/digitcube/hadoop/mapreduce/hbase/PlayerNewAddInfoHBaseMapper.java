package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 主要逻辑
 * 
 * 把玩家的新增日期、首付日期入到 HBase 表中
 * 
 */

public class PlayerNewAddInfoHBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	public static final String TB_USER_NEWADD = "user_newadd_info";
	public static final byte[] info = Bytes.toBytes("info");
	public static final byte[] fstLgDay = Bytes.toBytes("fstLgDay");//firstLoginDay
	public static final byte[] fstPayDay = Bytes.toBytes("fstPayDay");//firstPayDay
	public static final byte[] TB_USER_NEWADD_INFO = Bytes.toBytes(TB_USER_NEWADD);
	
	private ImmutableBytesWritable table = new ImmutableBytesWritable(TB_USER_NEWADD_INFO);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] array = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;	
			
		String pureAppId = array[i++];
		String platform = array[i++];
		String gameServer = array[i++];
		String accountId = array[i++];
		int firstLoginDay = StringUtil.convertInt(array[i++],0);
		int firstPayDay = StringUtil.convertInt(array[i++],0);
		
		if(firstLoginDay <=0 && firstPayDay <= 0){
			return;
		}
		
		//current row
		String rowKey = pureAppId + "|" + platform + "|" + gameServer + "|" + accountId;
		Put put = new Put(Bytes.toBytes(rowKey));
		if(firstLoginDay > 0){
			put.add(info, fstLgDay, Bytes.toBytes(firstLoginDay));
		}
		if(firstPayDay > 0){
			put.add(info, fstPayDay, Bytes.toBytes(firstPayDay));
		}
		//write to table
		context.write(table, put);
	}
}
