package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 数据仓库：付费表Reducer
 * 这里只是简单的对map输出的文件去重合并
 * 
 * @author sam.xie
 * @date 2015年4月17日 下午6:04:31
 * @version 1.0
 */
@Deprecated
public class WarehousePaymentReducer extends
		Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {
		context.write(key, NullWritable.get());
	}
}
