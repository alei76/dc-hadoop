package net.digitcube.hadoop.mapreduce.warehouse;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 
 * 输入1：在线日志
 * Key：			UID,AppID,Platform,LoginTime
 * Key.Suffix：	WAREHOUSE_ONLINE
 * Value：		Duration,ReceiveTime,IP,Country,Province,Brand,Resolution,OS,IMEI
 * 
 * 输入2：识别码
 * Key：			UID,MAC,IMEI
 * Key.Suffix：	WAREHOUSE_UID
 * Value：		U
 * 
 * 
 * 输出1：完整的登录在线记录
 * Duration取最大的在线时长
 * ReceiveTime取第一次接收时间，IP，Country，Province，Brand，Resolution，OS，IMEI都取第一次接收的值
 * Key：			UID,AppID,Platform,LoginTime
 * Key.Suffix：	WAREHOUSE_ONLINE
 * Value：		Duration,ReceiveTime,IP,Country,Province,Brand
 * 
 * 
 * 输出2：识别码
 * Key：			UID,MAC,IMEI
 * Key.Suffix：	WAREHOUSE_UID
 * Value：		
 * 
 * @author sam.xie
 * @date 2015年4月16日 下午3:23:33
 * @version 1.0
 */
@Deprecated
public class WarehouseOnlineReducer extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel keyFields = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		if (Constants.SUFFIX_WAREHOUSE_ONLINE.equals(key.getSuffix())) {
			int firstReceiveTime = -1; // 保存最早的时间戳
			int maxDuration = -1;// 保存最大的在线时长
			String[] tmpOnline = null;
			String tmpIp = "-";
			String tmpCountry = "-";
			String tmpProvince = "-";
			String tmpBrand = "-";
			String tmpResolution = "-";
			String tmpOS = "-";
			String tmpIMEI = "-";
			String[] keyArray = key.getOutFields();
			for (OutFieldsBaseModel val : values) {
				String[] valArray = val.getOutFields();
				int duration = StringUtil.convertInt(valArray[0], 0);
				int receiveTime = StringUtil.convertInt(valArray[1], 0);
				String ip = valArray[2];
				String country = valArray[3];
				String province = valArray[4];
				String brand = valArray[5];
				String resolution = valArray[6];
				String os = valArray[7];
				String imei = valArray[8];
				if (maxDuration < duration) {
					maxDuration = duration;
				}

				if (firstReceiveTime == -1 || firstReceiveTime > receiveTime) { // 取最早上报的ip，品牌等信息
					firstReceiveTime = receiveTime;
					tmpIp = ip;
					tmpCountry = country;
					tmpProvince = province;
					tmpBrand = brand;
					tmpResolution = brand;
					tmpBrand = brand;
					tmpResolution = resolution;
					tmpOS = os;
					tmpIMEI = imei;
				}
			}
			tmpOnline = new String[] { keyArray[0], keyArray[1], keyArray[2], keyArray[3], maxDuration + "",
					firstReceiveTime + "", tmpIp, tmpCountry, tmpProvince, tmpBrand, tmpResolution, tmpOS, tmpIMEI };
			// 写入去重后的在线记录
			keyFields.setSuffix(Constants.SUFFIX_WAREHOUSE_ONLINE);
			keyFields.setOutFields(tmpOnline);
			context.write(keyFields, NullWritable.get());
		} else if (Constants.SUFFIX_WAREHOUSE_UID.equals(key.getSuffix())) {
			context.write(key, NullWritable.get());
		}
	}
}
