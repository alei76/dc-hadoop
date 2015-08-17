package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 
 * 输入：@see WarehouseAppListMapper
 * 
 * 输出1：一个UID的安装过的应用包名记录
 * Key：			UID
 * Value：		pkgName1,pkgName2...
 * 
 * 输出1：一个UID的安装过的应用包名记录，取版本最大的appName,version
 * Key：			pkgName
 * Value：		appName,version
 * 
 * @author sam.xie
 * @date 2015年4月16日 下午3:23:33
 * @version 1.0
 */
public class WarehouseAppListReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private static int MAX_LENGTH = 65536;
	private Set<String> tmpPkgSet = new HashSet<String>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		if (Constants.SUFFIX_WAREHOUSE_APPLIST.equals(key.getSuffix())) {
			tmpPkgSet.clear();
			StringBuilder allPkgNames = new StringBuilder("");
			for (OutFieldsBaseModel val : values) {
				String pkgName = val.getOutFields()[0];
				if ((!StringUtil.isEmpty(pkgName)) && (!tmpPkgSet.contains(pkgName))) {
					tmpPkgSet.add(pkgName);
					allPkgNames.append(pkgName + ","); // 使用","分割
				}
			}

			if (allPkgNames.toString().getBytes().length > MAX_LENGTH || tmpPkgSet.size() == 0) {
				return;
			}
			String pkgs = allPkgNames.substring(0, allPkgNames.length() - 1); // 去掉最后的逗号
			valFields.setOutFields(new String[] { pkgs });
			context.write(key, valFields);
		} else if (Constants.SUFFIX_WAREHOUSE_APPDETAIL.equals(key.getSuffix())) {
			String maxVersion = "";
			String tmpAppName = "";
			for (OutFieldsBaseModel val : values) {
				String appName = val.getOutFields()[0];
				String version = val.getOutFields()[1];
				if (maxVersion.compareTo(version) < 0) {
					maxVersion = version;
					tmpAppName = appName;
				}
				tmpAppName = StringUtil.isEmpty(tmpAppName) ? appName : tmpAppName;
				maxVersion = StringUtil.isEmpty(maxVersion) ? version : maxVersion;
			}
			valFields.setOutFields(new String[] { tmpAppName, maxVersion });
			context.write(key, valFields);
		}
	}
}
