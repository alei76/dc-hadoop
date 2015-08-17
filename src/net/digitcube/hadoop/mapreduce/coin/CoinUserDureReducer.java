package net.digitcube.hadoop.mapreduce.coin;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 当天消费虚拟币的玩家数量去重 Reducer
 * Title: CoinUserDureReducer.java<br>
 * Description: CoinUserDureReducer.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author mikefeng     <br>
 * @date 2014-08-19         <br>
 * @version 1.0
 * <br>  
 * 
 * Reduce 输出
 * [appID, platform, channel, gameServer, accountId, coinType]
 */

public class CoinUserDureReducer extends Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {	
		context.write(key, NullWritable.get());
	}
	
}
