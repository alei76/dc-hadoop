package net.digitcube.hadoop.mapreduce.errorreport;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.mapreduce.Reducer;
import org.hsqldb.lib.StringUtil;

/**
 * @author seonzhang
 * 
 */
public class ErrorReportReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapValueObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		OutFieldsBaseModel firstValue = values.iterator().next();
		if ("A".equals(firstValue.getOutFields()[0])) { // 计算错误日志详情
			int errorTime = Integer.valueOf(firstValue.getOutFields()[1]);
			String errorTitle = firstValue.getOutFields()[2];
			String errorContent = firstValue.getOutFields()[3];
			while (values.iterator().hasNext()) {
				String[] valueArr = values.iterator().next().getOutFields();
				int tmpErrorTime = Integer.valueOf(valueArr[1]);
				String tmpErrorTitle = valueArr[2];
				String tmpErrorContent = valueArr[3];
				errorTime = errorTime > tmpErrorTime ? errorTime : tmpErrorTime;
				errorTitle = tmpErrorTitle;
				errorContent = "-".equals(tmpErrorContent) ? errorContent
						: tmpErrorContent;
			}
			// 加强判断 避免空的异常入库 seon 2014/9/9
			if (StringUtil.isEmpty(errorContent) || "-".equals(errorContent)) {
				return;
			}
			mapValueObj.setOutFields(new String[] { errorTime + "", errorTitle,
					errorContent });
			key.setSuffix(Constants.SUFFIX_ERROR_REPORT_DETAIL);
			context.write(key, mapValueObj);
		} else { // 计算次数
			int errorCount = 1;
			while (values.iterator().hasNext()) {
				values.iterator().next();
				errorCount++;
			}
			mapValueObj.setOutFields(new String[] { errorCount + "" });
			key.setSuffix(Constants.SUFFIX_ERROR_REPORT_DIST);
			context.write(key, mapValueObj);
		}

	}
}
