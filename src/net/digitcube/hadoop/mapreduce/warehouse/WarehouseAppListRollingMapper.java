package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * <pre>
 * 数据仓库：应用滚存列表
 * 
 * 输入：@see WarehouseAppListReducer
 * 1.应用列表天统计：	WAREHOUSE_APPLIST
 * 2.应用列表历史滚存：	WAREHOUSE_APPLIST_ROLLING
 * 3.应用详情天统计：	WAREHOUSE_APPDETAIL
 * 4.应用详情历史滚存：	WAREHOUSE_APPDETAIL_ROLLING
 * 
 * 输出：
 * 1.UID的所有应用列表
 * Key：			UID
 * Value：		pkgName1,pkgName2...
 * 
 * 2.应用详情
 * Key：			pkgName
 * Value：		appName,version
 * 
 * @author sam.xie
 * @date 2015年4月30 下午6:39:22
 * @version 1.0
 */
public class WarehouseAppListRollingMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private static int MAX_LENGTH = 65536;
	private static final String UTF8 = "UTF-8";
	private String fileSuffix = "";

	protected void setup(Context context) {
		fileSuffix = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paramsArr = value.toString().split(MRConstants.SEPERATOR_IN);
		if (fileSuffix.contains(Constants.SUFFIX_WAREHOUSE_APPLIST)
				|| fileSuffix.contains(Constants.SUFFIX_WAREHOUSE_APPLIST_ROLLING)) {
			String uid = paramsArr[0];
			String pkgList = paramsArr[1];
			if (StringUtil.isEmpty(pkgList) || pkgList.getBytes(UTF8).length > MAX_LENGTH) {
				return;
			}
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APPLIST_ROLLING);
			keyFields.setOutFields(new String[] { uid });
			valFields.setOutFields(new String[] { pkgList });
			context.write(keyFields, valFields);
		} else if (fileSuffix.contains(Constants.SUFFIX_WAREHOUSE_APPDETAIL)
				|| fileSuffix.contains(Constants.SUFFIX_WAREHOUSE_APPDETAIL_ROLLING)) {
			String pkgName = paramsArr[0];
			String appName = paramsArr[1];
			String version = paramsArr[2];
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_APPDETAIL_ROLLING);
			keyFields.setOutFields(new String[] { pkgName });
			valFields.setOutFields(new String[] { appName, version });
			context.write(keyFields, valFields);
		}
	}
}
