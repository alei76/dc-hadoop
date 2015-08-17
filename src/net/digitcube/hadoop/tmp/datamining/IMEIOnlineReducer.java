package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 获取在线日志中的IMEI统计
 * 
 * @author sam.xie
 * @date 2015年4月7日 下午4:34:33
 * @version 1.0
 */
public class IMEIOnlineReducer extends Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
