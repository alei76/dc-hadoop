package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.TreeSet;

import net.digitcube.hadoop.common.ConfigManager;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.OnlineAndPay;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHConstants;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * @see WHUidRollingMapper
 * 
 * @author sam.xie
 * @date 2015年7月30日 下午3:23:33
 * @version 1.0
 */
public class WHUidStatDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valFields = new OutFieldsBaseModel();
	private TreeSet<OnlineAndPay> sortedSet = new TreeSet<OnlineAndPay>(new Comparator<OnlineAndPay>() {
		@Override
		public int compare(OnlineAndPay o1, OnlineAndPay o2) {
			if (o1.getDuration() > o2.getDuration()) {
				return -1;
			} else if (o1.getDuration() < o2.getDuration()) {
				return 1;
			} else {
				// 这里注意，TreeMap通过Comparator来判断对象重复，在duration一致的情况下，按自然顺序比较游戏类型
				return o1.getKeys()[0].compareTo(o2.getKeys()[0]);
			}
		}
	});
	private Date scheduleTime = null;
	private int statTime = 0;

	@Override
	protected void setup(Context context) {
		scheduleTime = ConfigManager.getInitialDate(context.getConfiguration(), new Date());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(scheduleTime);
		calendar.add(Calendar.DAY_OF_MONTH, -1);// 结算时间默认调度时间的前一天
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		statTime = (int) (calendar.getTimeInMillis() / 1000);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String keySuffix = key.getSuffix();

		String[] total = new String[15];
		String[][] hourDuration = new String[3][24]; // 汇总|工作日|周末 24小时在线时长
		String[][] hourPayTimes = new String[3][24]; // 汇总|工作日|周末 24小时登录次数
		String[][] appHabit = new String[5][8];
		initTotalArray(total);
		initHourArray(hourDuration);
		initHourArray(hourPayTimes);
		initAppHabitArray(appHabit);

		String[] mergeArray = null;
		for (OutFieldsBaseModel value : values) {
			String[] valArr = value.getOutFields();
			if (keySuffix.equals(Constants.SUFFIX_WAREHOUSE_UID_STAT_DAY)) {
				String valSuffix = value.getSuffix();
				if ("R".equals(valSuffix)) {
					total = valArr;
				} else if ("H".equals(valSuffix)) {
					// 小时合并
					// String uid = valArr[0];
					int weekType = StringUtil.convertInt(valArr[1], 0);
					int dim = StringUtil.convertInt(valArr[2], 0);
					if (WHConstants.TYPE_ALL == weekType) {
						if (WHConstants.DIM_DURATION == dim) {
							System.arraycopy(valArr, 4, hourDuration[0], 0, 24);
						} else if (WHConstants.DIM_PAY_TIMES == dim) {
							System.arraycopy(valArr, 4, hourPayTimes[0], 0, 24);
						}
					} else if (WHConstants.TYPE_WEEKDAY == weekType) {
						if (WHConstants.DIM_DURATION == dim) {
							System.arraycopy(valArr, 4, hourDuration[1], 0, 24);
						} else if (WHConstants.DIM_PAY_TIMES == dim) {
							System.arraycopy(valArr, 4, hourPayTimes[1], 0, 24);
						}
					} else if (WHConstants.TYPE_WEEKEND == weekType) {
						if (WHConstants.DIM_DURATION == dim) {
							System.arraycopy(valArr, 4, hourDuration[2], 0, 24);
						} else if (WHConstants.DIM_PAY_TIMES == dim) {
							System.arraycopy(valArr, 4, hourPayTimes[2], 0, 24);
						}
					}
				} else if ("A".equals(valSuffix)) {
					// app类型取在线时长最多的前5
					OnlineAndPay item = new OnlineAndPay(2, valArr);
					sortedSet.add(item);
				}
			} else if (keySuffix.equals(Constants.SUFFIX_WAREHOUSE_UIDAPP_STAT_DAY)) {
				String valSuffix = value.getSuffix();
				if ("R".equals(valSuffix)) {
					total = valArr;
				} else if ("H".equals(valSuffix)) {
					// 小时合并
					// String uid = valArr[0];
					// String app = valArr[1];
					int weekType = StringUtil.convertInt(valArr[2], 0);
					int dim = StringUtil.convertInt(valArr[3], 0);
					if (WHConstants.TYPE_ALL == weekType) {
						if (WHConstants.DIM_DURATION == dim) {
							System.arraycopy(valArr, 5, hourDuration[0], 0, 24);
						} else if (WHConstants.DIM_PAY_TIMES == dim) {
							System.arraycopy(valArr, 5, hourPayTimes[0], 0, 24);
						}
					} else if (WHConstants.TYPE_WEEKDAY == weekType) {
						if (WHConstants.DIM_DURATION == dim) {
							System.arraycopy(valArr, 5, hourDuration[1], 0, 24);
						} else if (WHConstants.DIM_PAY_TIMES == dim) {
							System.arraycopy(valArr, 5, hourPayTimes[1], 0, 24);
						}
					} else if (WHConstants.TYPE_WEEKEND == weekType) {
						if (WHConstants.DIM_DURATION == dim) {
							System.arraycopy(valArr, 5, hourDuration[2], 0, 24);
						} else if (WHConstants.DIM_PAY_TIMES == dim) {
							System.arraycopy(valArr, 5, hourPayTimes[2], 0, 24);
						}
					}
				}
			}
		}
		if (keySuffix.equals(Constants.SUFFIX_WAREHOUSE_UID_STAT_DAY)) {
			for (int i = 0; i < 5; i++) {
				OnlineAndPay typeItem = sortedSet.pollFirst();
				if (null != typeItem) {
					appHabit[i][0] = typeItem.getKeys()[1];
					int validDays = (statTime - typeItem.getFirstLoginTime()) / WHConstants.SECONDS_IN_ONE_DAY;
					appHabit[i][1] = validDays + "";
					appHabit[i][2] = typeItem.getLoginTimes() + "";
					appHabit[i][3] = typeItem.getDuration() + "";
					appHabit[i][4] = typeItem.getPayTimes() + "";
					appHabit[i][5] = typeItem.getPayAmount() + "";
					appHabit[i][6] = typeItem.getLastLoginTime() + "";
					appHabit[i][7] = typeItem.getLastPayTime() + "";
				}
			}
			mergeArray = StringUtil.merge(total, hourDuration[0], hourPayTimes[0], hourDuration[1], hourPayTimes[1],
					hourDuration[2], hourPayTimes[2], appHabit[0], appHabit[1], appHabit[2], appHabit[3], appHabit[4]);
		} else if (keySuffix.equals(Constants.SUFFIX_WAREHOUSE_UIDAPP_STAT_DAY)) {
			mergeArray = StringUtil.merge(total, hourDuration[0], hourPayTimes[0], hourDuration[1], hourPayTimes[1],
					hourDuration[2], hourPayTimes[2]);
		}

		valFields.setOutFields(mergeArray);
		context.write(key, valFields);
	}

	private void initTotalArray(String[] array) {
		for (int j = 0; j < array.length; j++) {
			array[j] = "0";
		}

	}

	private void initHourArray(String[][] arrays) {
		for (int i = 0; i < arrays.length; i++) {
			for (int j = 0; j < arrays[i].length; j++) {
				arrays[i][j] = "0";
			}
		}
	}

	private void initAppHabitArray(String[][] arrays) {
		for (int i = 0; i < arrays.length; i++) {
			arrays[i][0] = "-";
			for (int j = 1; j < arrays[i].length; j++) {
				arrays[i][j] = "0";
			}
		}
	}

}
