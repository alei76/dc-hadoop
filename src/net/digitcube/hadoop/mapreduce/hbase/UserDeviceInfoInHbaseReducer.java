package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.jce.PlayerDeviceInfo;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public class UserDeviceInfoInHbaseReducer
		extends
		TableReducer<OutFieldsBaseModel, OutFieldsBaseModel, ImmutableBytesWritable> {

	public void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {
		String[] keyFields = key.getOutFields();
		String appid = keyFields[0];
		String platform = keyFields[1];
		String accountId = keyFields[2];
		String uid = keyFields[3];
		int lastLoginTime = 0;
		String[] valueFields = null;
		for (OutFieldsBaseModel val : values) {
			String[] outFields = val.getOutFields();
			int loginTime = Integer.parseInt(outFields[0]);
			if (loginTime > lastLoginTime) {
				valueFields = outFields;
				lastLoginTime = loginTime;
			}
		}
		if (valueFields == null)
			return;

		int accountType = Integer.parseInt(valueFields[1]);
		int gender = Integer.parseInt(valueFields[2]);
		int age = Integer.parseInt(valueFields[3]);
		String resolution = valueFields[4];
		String opSystem = valueFields[5];
		String brand = valueFields[6];
		int netType = Integer.parseInt(valueFields[7]);
		String country = valueFields[8];
		String province = valueFields[9];
		String operators = valueFields[10];

		PlayerDeviceInfo deviceInfo = new PlayerDeviceInfo();
		deviceInfo.setBrand(brand);
		deviceInfo.setResolution(resolution);
		deviceInfo.setOpersystem(opSystem);
		deviceInfo.setCountry(country);
		deviceInfo.setProvince(province);
		deviceInfo.setCarrier(operators);
		deviceInfo.setNetType(netType);
		deviceInfo.setGender(gender);
		deviceInfo.setAge(age);
		deviceInfo.setAccountType(accountType);
		String hbaseKey = appid + "|" + platform + "|" + accountId;
		Put putrow = new Put(hbaseKey.getBytes());
		putrow.add("info".getBytes(), uid.getBytes(), deviceInfo.toByteArray());
		context.write(new ImmutableBytesWritable(hbaseKey.getBytes()), putrow);
	}
}
