package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：在线表统计
 * 输入：
 * 1.识别码记录WAREHOUSE_UID，@see WarehouseOnlineMapper
 * 2.识别码历史滚存日志WAREHOUSE_UID_ROLLING
 * 
 * 
 * 输出：
 * Key:			UID,MAC,IMEI
 * Key.Suffix	WAREHOUSE_UID_ROLLING
 * Value：		
 * 
 * @author sam.xie
 * @date 2015年4月20日 下午2:39:22
 * @version 1.0
 */
@Deprecated
public class WarehouseUIDRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		// 识别码滚存
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_DEVICE_ROLLING);
		keyFields.setOutFields(paramsArr);
		context.write(keyFields, NullWritable.get());
	}
}
