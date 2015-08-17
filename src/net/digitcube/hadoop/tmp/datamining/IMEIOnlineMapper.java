package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.OnlineLog;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 获取在线日志中的IMEI统计
 * 
 * @author sam.xie
 * @date 2015年4月7日 下午4:34:33
 * @version 1.0
 */
public class IMEIOnlineMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyObj = new OutFieldsBaseModel();
	private String yyyyMMdd = "";

	protected void setup(Context context) {
		Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
		// String fileName = inputPath.getName();
		int yearIndex = context.getConfiguration().getInt("year.index.in.path", 4);
		String input = inputPath.toString();
		input = input.replace("://", ":");
		String[] components = input.split("/");
		yyyyMMdd = components[yearIndex] + components[yearIndex + 1] + components[yearIndex + 2];
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		OnlineLog log = null;
		try {
			log = new OnlineLog(paraArr);
		} catch (Exception e) {
			return;
		}
		String appID = log.getAppID();
		String platform = log.getPlatform();
		String imei = log.getExt5Map().get("IMEI");
		if (StringUtils.isNotBlank(appID) && appID.contains("144ECD682E57355D18B70460BD82C07F") && "2".equals(platform)) {
			imei = StringUtils.isBlank(imei) ? "UNKNOW_IMEI" : imei;
			keyObj.setSuffix(yyyyMMdd);
			keyObj.setOutFields(new String[] { imei });
			context.write(keyObj, NullWritable.get());
		}

	}

}
