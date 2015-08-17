package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 每日虚拟币汇总 Map-2
 * Title: CoinLostOrGainMapper.java<br>
 * Description: CoinLostOrGainMapper.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2013-10-31         <br>
 * @version 1.0
 * <br>
 * 
 * 输入：MapReduce-1产生的文件
 * suffix:TOTAL_COIN_EACH_ACCOUNT
 * [appID, platform, channel, gameServer,accountID,CoinNum]
 * 
 * Map:
 * 输出
 * key: [appID, platform, channel, gameServer]
 * value: coinNum
 */
/**该部分功能合并到 @CoinMap 中处理*/
@Deprecated
public class TotalCoinMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private IntWritable valObj = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		String[] keyFields = null;
		String appID = paraArr[0];
		String platform = paraArr[1];
		String channel = paraArr[2];
		String gameServer = paraArr[3];
		int coinNum = StringUtil.convertInt(paraArr[5], 0);

		keyFields = new String[] { appID, platform, channel, gameServer };
		keyObj.setOutFields(keyFields);
		valObj.set(coinNum);

		context.write(keyObj, valObj);
	}
}
