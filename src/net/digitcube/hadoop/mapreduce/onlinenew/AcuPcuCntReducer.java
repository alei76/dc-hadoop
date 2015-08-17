package net.digitcube.hadoop.mapreduce.onlinenew;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author rickpan
 * @version 1.0 
 * 
 * 逻辑：@AcuPcuCntHourMapper
 * 
 */

public class AcuPcuCntReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {

	private IntWritable vaule = new IntWritable();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		/*
		 * 计算 ACU/PCU 每个时间点在线人数
		 * 如果是小时在线玩家去重，则只输出 1
		 */
		String dataFlag = key.getOutFields()[key.getOutFields().length - 1];
		int total = 0;
		if(Constants.DATA_FLAG_CNT.equals(dataFlag)){
			total = 1;
		}else{
			for (IntWritable val : values) {
				total += val.get();
			}
		}
		
		vaule.set(total);
		key.setSuffix(Constants.SUFFIX_ACU_PCU_CNT);
		
		context.write(key, vaule);
	}

}