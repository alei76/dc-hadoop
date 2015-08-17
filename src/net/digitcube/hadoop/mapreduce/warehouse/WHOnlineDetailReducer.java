package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHConstants;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHDataUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see WHOnlineDetailMapper
 * 
 * @author sam.xie
 * @date 2015年6月16日 下午3:23:33
 * @version 1.0
 */
public class WHOnlineDetailReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		if (Constants.SUFFIX_WAREHOUSE_ONLINE_DETAIL.equals(key.getSuffix())) {
			int maxDuration = 0;// 保存最大的在线时长
			int maxLevel = 0; // 保存最大的级别
			String[] lastValArray = null;
			for (OutFieldsBaseModel val : values) {
				lastValArray = val.getOutFields();
				int level = StringUtil.convertInt(lastValArray[WHConstants.IDX_LEVEL], 1);
				int duration = StringUtil.convertInt(lastValArray[WHConstants.IDX_ONLINE_DURATION], 0);
				if (maxDuration < duration) {
					maxDuration = duration;
				}

				if (maxLevel < level) {
					maxLevel = level;
				}
			}
			lastValArray[WHConstants.IDX_LEVEL] = maxLevel + "";
			lastValArray[WHConstants.IDX_ONLINE_DURATION] = maxDuration + "";
			// 数据清理
			String appId = key.getOutFields()[2];
			String uid = key.getOutFields()[0];
			lastValArray[WHConstants.IDX_CLAENFLAG] = WHDataUtil.keywordFilter(appId, uid);
			// 使用最后一条记录作为输出
			key.setOutFields(lastValArray);
			context.write(key, NullWritable.get());
		}
	}
}
