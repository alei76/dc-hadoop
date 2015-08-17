package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.IOSChannelUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 输入：@see PaymentTypeDayMapper
 * 
 * 输出：
 * 1.付费类型
 * Key			AppID,Platform,Channel,GameServer,PayType,Amount		
 * Key.Suffix	PAYMENT_PAYTYPE_DAY
 * Value		NULL
 * 
 * 2.付费点（金额）
 * Key			AppID,Platform,Channel,GameServer,IAPID,Amount
 * Key.Suffix	PAYMENT_POINT_AMOUNT_DAY
 * Value		NULL
 * 
 * 3.付费点（人数）
 * Key			AppID,Platform,Channel,GameServer,IAPID,Num
 * Key.Suffix	PAYMENT_POINT_NUM_DAY
 * Value		NULL
 * 
 * 3.付费货币（原始金额）
 * Key			AppID,Platform,Channel,GameServer,IAPID,OriginalAmount
 * Key.Suffix	PAYMENT_CURRENCY_ORIGINAL_DAY
 * Value		NULL
 * 
 * 4.付费货币（最终金额）
 * Key			AppID,Platform,Channel,GameServer,IAPID,FinalAmount
 * Key.Suffix	PAYMENT_CURRENCY_FINAL_DAY
 * Value		NULL
 */
public class PaymentTypeDayReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	public static int Index_Key_AppId = 0;
	public static int Index_Key_PlateForm = 1;
	public static int Index_Key_Channel = 2;
	public Set<String> accountSet = new HashSet<String>();

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		IOSChannelUtil.close();
	}

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		accountSet.clear();
		if (Constants.SUFFIX_PAYMENT_PAYTYPE_DAY.equals(key.getSuffix())) { // 付费类型
			float totalCurrencyAmount = 0;
			String UID = ""; // iOS 渠道修正用
			String[] keyArray = key.getOutFields();
			for (OutFieldsBaseModel val : values) {
				UID = val.getSuffix();
				String[] value = val.getOutFields();
				totalCurrencyAmount += StringUtil.convertFloat(value[0], 0);
			}

			// iOS 渠道修正
			// 20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
			// if(MRConstants.PLATFORM_iOS_STR.equals(keyArray[Index_Key_PlateForm])){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(keyArray[Index_Key_AppId], UID,
					keyArray[Index_Key_PlateForm], keyArray[Index_Key_Channel]);
			keyArray[Index_Key_Channel] = reviseChannel;
			// }
			String[] keyFields = new String[keyArray.length + 1];
			System.arraycopy(keyArray, 0, keyFields, 0, keyArray.length);
			keyFields[keyFields.length - 1] = totalCurrencyAmount + "";
			key.setOutFields(keyFields);
			context.write(key, NullWritable.get());
		} else if (Constants.SUFFIX_PAYMENT_POINT_DAY.equals(key.getSuffix())) { // 付费点
			float totalCurrencyAmount = 0;
			int totalPayNum = 0; // 付费人数
			String UID = ""; // iOS 渠道修正用
			String maxVersion = "";// 对同一个玩家不同版本信息进行揉合，取最大版本号
			String[] keyArray = key.getOutFields();
			for (OutFieldsBaseModel val : values) {
				UID = val.getSuffix();
				String[] value = val.getOutFields();
				float currencyAmount = StringUtil.convertFloat(value[0], 0);
				totalCurrencyAmount += currencyAmount;
				// 取最大版本号
				String currentVerion = value[1];
				String accountId = value[2];
				if (maxVersion.compareTo(currentVerion) < 0) {
					maxVersion = currentVerion;
				}
				if (!accountSet.contains(accountId)) {
					accountSet.add(accountId);
					totalPayNum += 1;
				}
			}

			keyArray[Index_Key_AppId] = keyArray[Index_Key_AppId] + "|" + maxVersion;
			// iOS 渠道修正
			// 20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
			// if(MRConstants.PLATFORM_iOS_STR.equals(keyArray[Index_Key_PlateForm])){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(keyArray[Index_Key_AppId], UID,
					keyArray[Index_Key_PlateForm], keyArray[Index_Key_Channel]);
			keyArray[Index_Key_Channel] = reviseChannel;
			// }
			String[] keyFields = new String[keyArray.length + 1];
			System.arraycopy(keyArray, 0, keyFields, 0, keyArray.length);
			// 付费点-金额
			key.setSuffix(Constants.SUFFIX_PAYMENT_POINT_AMOUNT_DAY);
			keyFields[keyFields.length - 1] = totalCurrencyAmount + "";
			key.setOutFields(keyFields);
			context.write(key, NullWritable.get());

			// 付费点-人数
			key.setSuffix(Constants.SUFFIX_PAYMENT_POINT_NUM_DAY);
			keyFields[keyFields.length - 1] = totalPayNum + "";
			key.setOutFields(keyFields);
			context.write(key, NullWritable.get());

		} else if (Constants.SUFFIX_PAYMENT_CURRENCY_DAY.equals(key.getSuffix())) { // 付费货币
			float totalOriginalAmount = 0;
			float totalFinalAmount = 0; // 付费人数
			String UID = ""; // iOS 渠道修正用
			String maxVersion = "";// 对同一个玩家不同版本信息进行揉合，取最大版本号
			String[] keyArray = key.getOutFields();
			for (OutFieldsBaseModel val : values) {
				UID = val.getSuffix();
				String[] value = val.getOutFields();
				float originalAmount = StringUtil.convertFloat(value[0], 0);
				float finalAmount = StringUtil.convertFloat(value[1], 0);
				totalOriginalAmount += originalAmount;
				totalFinalAmount += finalAmount;
				// 取最大版本号
				String currentVerion = value[2];
				if (maxVersion.compareTo(currentVerion) < 0) {
					maxVersion = currentVerion;
				}

			}

			keyArray[Index_Key_AppId] = keyArray[Index_Key_AppId] + "|" + maxVersion;
			// iOS 渠道修正
			// 20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
			// if(MRConstants.PLATFORM_iOS_STR.equals(keyArray[Index_Key_PlateForm])){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(keyArray[Index_Key_AppId], UID,
					keyArray[Index_Key_PlateForm], keyArray[Index_Key_Channel]);
			keyArray[Index_Key_Channel] = reviseChannel;
			// }
			String[] keyFields = new String[keyArray.length + 1];
			System.arraycopy(keyArray, 0, keyFields, 0, keyArray.length);
			// 付费货币-原始金额
			key.setSuffix(Constants.SUFFIX_PAYMENT_CURRENCY_ORIGINAL_DAY);
			keyFields[keyFields.length - 1] = totalOriginalAmount + "";
			key.setOutFields(keyFields);
			context.write(key, NullWritable.get());

			// 付费货币-最终金额
			key.setSuffix(Constants.SUFFIX_PAYMENT_CURRENCY_FINAL_DAY);
			keyFields[keyFields.length - 1] = totalFinalAmount + "";
			key.setOutFields(keyFields);
			context.write(key, NullWritable.get());
		} else if (Constants.SUFFIX_PAYMENT_SIMCODE_DAY.equals(key.getSuffix())) { // 运营商代号
			float totalCurrencyAmount = 0;
			String UID = ""; // iOS 渠道修正用
			String[] keyArray = key.getOutFields();
			for (OutFieldsBaseModel val : values) {
				UID = val.getSuffix();
				String[] value = val.getOutFields();
				float currencyAmount = StringUtil.convertFloat(value[0], 0);
				totalCurrencyAmount += currencyAmount;
			}

			// iOS 渠道修正
			// 20141202 : 对某个特定的渠道，除 iOS 外，其它平台也需进行渠道匹配
			// if(MRConstants.PLATFORM_iOS_STR.equals(keyArray[Index_Key_PlateForm])){
			String reviseChannel = IOSChannelUtil.checkForiOSChannel(keyArray[Index_Key_AppId], UID,
					keyArray[Index_Key_PlateForm], keyArray[Index_Key_Channel]);
			keyArray[Index_Key_Channel] = reviseChannel;
			// }
			String[] keyFields = new String[keyArray.length + 1];
			System.arraycopy(keyArray, 0, keyFields, 0, keyArray.length);
			keyFields[keyFields.length - 1] = totalCurrencyAmount + "";
			key.setSuffix(Constants.SUFFIX_PAYMENT_SIMCODE_DAY);
			key.setOutFields(keyFields);
			context.write(key, NullWritable.get());
		}
	}

}
