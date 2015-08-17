package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 
 * 输入1：
 * Key：			UID,MAC,IMEI
 * Key.Suffix：	WAREHOUSE_UID_ROLLING
 * Value：		U
 * 
 * 
 * 输出1：识别码
 * Key：			UID,MAC,IMEI
 * Key.Suffix：	WAREHOUSE_UID_ROLLING
 * Value：		
 * 
 * @author sam.xie
 * @date 2015年4月16日 下午3:23:33
 * @version 1.0
 */
@Deprecated
public class WarehouseUIDRollingReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
