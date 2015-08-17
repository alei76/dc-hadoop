package net.digitcube.hadoop.mapreduce.errorreport;

import java.io.IOException;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class ErrorReportDayReducer extends
		Reducer<OutFieldsBaseModel, BigFieldsBaseModel, OutFieldsBaseModel, BigFieldsBaseModel> {
	
	private BigFieldsBaseModel mapValueObj = new BigFieldsBaseModel();

	private int statTime = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statTime = DateUtil.getStatDate(context.getConfiguration());
	}
	
	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<BigFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {				
		BigFieldsBaseModel firstValue = values.iterator().next();		
		
		if ("SYS".equals(firstValue.getSuffix())){
				if ("A".equals(firstValue.getOutFields()[0])) { // 计算错误日志详情
					//int errorTime = Integer.valueOf(firstValue.getOutFields()[1]);
					int errorTime = StringUtil.convertInt(firstValue.getOutFields()[1], statTime);
					String errorTitle = firstValue.getOutFields()[2];
					String errorContent = firstValue.getOutFields()[3];
					int errorCount = 1;
					while (values.iterator().hasNext()) {
						String[] valueArr = values.iterator().next().getOutFields();
						int tmpErrorTime = StringUtil.convertInt(valueArr[1], statTime);
						String tmpErrorTitle = valueArr[2];
						String tmpErrorContent = valueArr[3];
						errorTime = errorTime > tmpErrorTime ? errorTime : tmpErrorTime;
						errorTitle = tmpErrorTitle;
						errorContent = "-".equals(tmpErrorContent) ? errorContent : tmpErrorContent;
						errorCount ++;
					}
					// 加强判断 避免空的异常入库 
					if (StringUtil.isEmpty(errorContent) || "-".equals(errorContent)) {
						return;
					}					
					mapValueObj.setOutFields(new String[] {  errorTitle,errorContent,
							 errorCount + "",errorTime + "",});
					key.setSuffix(Constants.SUFFIX_ERROR_REPORT_DETAIL_SYS);
					context.write(key, mapValueObj);
				} else if("B".equals(firstValue.getOutFields()[0])){ // 计算次数
					int errorCount = 1;
					while (values.iterator().hasNext()) {
						values.iterator().next();
						errorCount++;
					}
					mapValueObj.setOutFields(new String[] { errorCount + "" });
					key.setSuffix(Constants.SUFFIX_ERROR_REPORT_DIST_SYS);
					context.write(key, mapValueObj);
				}
				
		}else if("USER".equals(firstValue.getSuffix())){
			
			if ("A".equals(firstValue.getOutFields()[0])) { // 计算错误日志详情
				int errorTime = StringUtil.convertInt(firstValue.getOutFields()[1], statTime);
				String errorTitle = firstValue.getOutFields()[2];
				String errorContent = firstValue.getOutFields()[3];
				int errorCount = 1;
				while (values.iterator().hasNext()) {
					String[] valueArr = values.iterator().next().getOutFields();
					int tmpErrorTime = StringUtil.convertInt(valueArr[1], statTime);
					String tmpErrorTitle = valueArr[2];
					String tmpErrorContent = valueArr[3];
					errorTime = errorTime > tmpErrorTime ? errorTime : tmpErrorTime;
					errorTitle = tmpErrorTitle;
					errorContent = "-".equals(tmpErrorContent) ? errorContent : tmpErrorContent;
					errorCount ++;
				}
				// 加强判断 避免空的异常入库
				if (StringUtil.isEmpty(errorContent) || "-".equals(errorContent)) {
					return;
				}
				mapValueObj.setOutFields(new String[] {  errorTitle,errorContent,
						 errorCount + "",errorTime + "",});
				key.setSuffix(Constants.SUFFIX_ERROR_REPORT_DETAIL_USER);
				context.write(key, mapValueObj);
			} else if("B".equals(firstValue.getOutFields()[0])){ // 计算次数
				int errorCount = 1;
				while (values.iterator().hasNext()) {
					values.iterator().next();
					errorCount++;
				}
				mapValueObj.setOutFields(new String[] { errorCount + "" });
				key.setSuffix(Constants.SUFFIX_ERROR_REPORT_DIST_USER);
				context.write(key, mapValueObj);
			}
			
		}

	}
}
