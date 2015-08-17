package net.digitcube.hadoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.HeaderLog;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 注意：
 * a) 需在任务参数中配置  dc.log.model.class,用于解析各种日志
 * b) 需在任务参数中配置  dc.log.name,用于输出文件前缀用
 * c) 需写一个新的 OutputFormat，以  dc.log.name 作为前缀输出
 */
public class AccountIdMergeMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	private Constructor constructor = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		try{
			String realClass = context.getConfiguration().get("dc.log.model.class");
			constructor = Class.forName(realClass).getConstructor(String[].class);
		}catch(Throwable t){
			throw new RuntimeException(t);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(MRConstants.SEPERATOR_IN);
		try {
			HeaderLog log = (HeaderLog)(constructor.newInstance((Object)fields));
			String appId = log.getAppID();
			String uid = log.getUID();
			String accountId = log.getAccountID();
			mapKeyObj.setOutFields(new String[]{
					appId,
					uid
			});
			mapValObj.setSuffix(accountId);
			mapValObj.setOutFields(fields);
			context.write(mapKeyObj, mapValObj);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}
}
