package net.digitcube.hadoop.mapreduce.app;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 *  key:
 *  { appID, platForm, channel, gameServerReal, netType, appName,fileType, category, appVer, appSize };
 *  Suffix(Constants.SUFFIX_DOWNLOAD_TIMES);
 *  value:one
 *  下载成功次数
 *  key:
 *  {appID, platForm, channel, gameServerReal, netType, appName,fileType, category, appVer, appSize};
 *  Suffix(Constants.SUFFIX_DOWNLOAD_SUCCESS_TIMES);
 *  value:one
 *  下载失败原因
 *  key:
 *  { appID, platForm, channel, gameServerReal, reason};
 * 	Suffix(Constants.SUFFIX_DOWNLOAD_FAIL_REASON);
 *  value: one
 *  下载中断原因
 * key:
 *  { appID, platForm, channel, gameServerReal, reason};
 * 	Suffix(Constants.SUFFIX_DOWNLOAD_INTERRUPT_REASON);
 *  value: one
 * Title: AppDownloadReducer.java<br>
 * Description: AppDownloadReducer.java<br>
 * Copyright: Copyright (c) 2013 www.dataeye.com<br>
 * Company: DataEye 数据之眼<br>
 * 
 * @author Ivan     <br>
 * @date 2014-6-9         <br>
 * @version 1.0
 * <br>
 */
public class AppDownloadReducer extends Reducer<OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> {
	private IntWritable sum = new IntWritable();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		String suffix = key.getSuffix();
		if (Constants.SUFFIX_DOWNLOAD_TIMES.equals(suffix) || Constants.SUFFIX_DOWNLOAD_SUCCESS_TIMES.equals(suffix)
				|| Constants.SUFFIX_DOWNLOAD_FAIL_REASON.equals(suffix)
				|| Constants.SUFFIX_DOWNLOAD_INTERRUPT_REASON.equals(suffix)
				|| Constants.SUFFIX_DOWNLOAD_COST.equals(suffix)) {
			int sumTmp = 0;
			for (IntWritable intWritable : values) {
				sumTmp += intWritable.get();
			}
			sum.set(sumTmp);
			context.write(key, sum);
		}
	}
}
