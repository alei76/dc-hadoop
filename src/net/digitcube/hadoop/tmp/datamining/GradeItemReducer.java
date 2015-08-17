package net.digitcube.hadoop.tmp.datamining;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.mapreduce.Reducer;

public class GradeItemReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private Set<String> tmpSet = new HashSet<String>();
	private Set<String> tmpSuccSet = new HashSet<String>();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		tmpSet.clear(); // 所有玩家去重
		tmpSuccSet.clear(); // 通关玩家去重
		String keySuffix = key.getSuffix();
		if ("GRADE".equals(keySuffix)) { // 通关统计
			int allPlayerCount = 0; // 所有关卡人数
			int succPlayerCount = 0; // 通关人数
			int succDuration = 0; // 通关时长
			int failTimes = 0; // 通关失败次数
			for (OutFieldsBaseModel val : values) {
				String valSuffix = val.getSuffix();
				if ("SUCC".equals(valSuffix)) { // 统计首次通关人数，通关时长
					String accountID = val.getOutFields()[0];
					int duration = Integer.parseInt(val.getOutFields()[1]);
					if (!tmpSuccSet.contains(accountID)) { // 通关去重
						tmpSuccSet.add(accountID);
						succPlayerCount++;
						succDuration += duration;
					}
					if (!tmpSet.contains(accountID)) { // 所有去重
						tmpSet.add(accountID);
						allPlayerCount++;
					}

				} else if ("FAIL".equals(valSuffix)) { // 通关失败次数
					String accountID = val.getOutFields()[0];
					if (!tmpSet.contains(accountID)) {
						tmpSet.add(accountID);
						allPlayerCount++;
					}
					failTimes++;
				}
			}
			context.write(key, new OutFieldsBaseModel(new String[] { succPlayerCount + "", failTimes + "",
					succDuration + "", allPlayerCount + "" }));
		} else if ("ITEM".equals(keySuffix)) { // 关卡道具使用
			int itemPlayerCount = 0;// 使用道具的玩家数
			for (OutFieldsBaseModel val : values) {
				String accountID = val.getOutFields()[0];
				if (!tmpSet.contains(accountID)) {
					tmpSet.add(accountID);
					itemPlayerCount++;
				}
			}
			context.write(key, new OutFieldsBaseModel(new String[] { itemPlayerCount + "" }));
		} else if ("STAR".equals(keySuffix)) {
			int starPlayerCount = 0;// 星级玩家数
			for (OutFieldsBaseModel val : values) {
				String accountID = val.getOutFields()[0];
				if (!tmpSet.contains(accountID)) {
					tmpSet.add(accountID);
					starPlayerCount++;
				}
			}
			context.write(key, new OutFieldsBaseModel(new String[] { starPlayerCount + "" }));
		}
	}
}
