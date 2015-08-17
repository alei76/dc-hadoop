package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.AppListLog;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * <pre>
 * 数据仓库：应用列表
 * 
 * 输入：
 * 1.应用详情日志AppDetail
 * 
 * 输出1：UID和包名
 * Key：			UID
 * Value：		pkgName
 * 
 * 输出2：应用元信息
 * Key：			pkgName
 * Value：		appName,version
 * 
 * @author sam.xie
 * @date 2015年4月17日 上午9:39:22
 * @version 1.0
 */
public class WarehouseAppListMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	// private String fileSuffix = null;
	private static int MAX_LENGTH = 32768; // 理论最大值65536
	private static final String UTF8 = "UTF-8";

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		AppListLog log = null;
		try {
			log = new AppListLog(paramsArr);
		} catch (Exception e) {
			return;
		}
		String uid = log.getUID();
		String pkgName = log.getPackageName();
		String appName = log.getAppName();
		String version = log.getAppVer();
		if (pkgName.getBytes(UTF8).length > MAX_LENGTH || StringUtil.isEmpty(pkgName)) {
			return;
		} else {
			appName = StringUtil.isEmpty(appName) || appName.getBytes(UTF8).length > MAX_LENGTH ? "-" : appName;
			version = StringUtil.isEmpty(version) || version.getBytes(UTF8).length > MAX_LENGTH ? "-" : version;
		}
		/* 1.写入uid,pkgName */
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APPLIST);
		keyFields.setOutFields(new String[] { uid });
		valFields.setOutFields(new String[] { pkgName });
		context.write(keyFields, valFields);

		/* 2.写入pkgName,appName,version */
		keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APPDETAIL);
		keyFields.setOutFields(new String[] { pkgName });
		valFields.setOutFields(new String[] { appName, version });
		context.write(keyFields, valFields);
	}

}
