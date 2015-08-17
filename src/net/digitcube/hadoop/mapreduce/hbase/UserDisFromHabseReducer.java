package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.util.Iterator;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserDisFromHabseReducer extends
		Reducer<OutFieldsBaseModel, Text, ImmutableBytesWritable, Put> {

	private static final byte[] cf = Bytes.toBytes("info");
	private static final byte[] qualifier = Bytes.toBytes("q");
	private ImmutableBytesWritable table = new ImmutableBytesWritable();

	public void reduce(OutFieldsBaseModel key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		String[] keyFields = key.getOutFields();
		String appid = keyFields[0];
		String platform = keyFields[1];
		String type1 = keyFields[2];// 例如 地区 ar
		String type2 = keyFields[3];// 例如 广东省
		String orderid = context.getConfiguration().get("hbase.orderid");
		String table_order_accounts = context.getConfiguration().get(
				"hbase.tablename.order_accounts");
		String table_order_dis = context.getConfiguration().get(
				"hbase.tablename.order_dis");
		int counts = 0;
		// 1... 写order_accounts表 orderid|分布大类|分布小类|APPID|PlatForm -
		// account1,account2...accountN
		String hbasekey = orderid + "|" + type1 + "|" + type2 + "|" + appid
				+ "|" + platform;
		// 拼接value
		StringBuilder sb = new StringBuilder();
		Iterator<Text> i = values.iterator();
		sb.append(i.next().toString());
		counts++;
		while (i.hasNext()) {
			sb.append(",");
			sb.append(i.next().toString());
			counts++;
		}
		byte[] valBytes = Bytes.toBytes(sb.toString());
		table.set(Bytes.toBytes(table_order_accounts));

		Put put = new Put(Bytes.toBytes(hbasekey));
		put.add(cf, qualifier, valBytes);
		context.write(table, put);

		// 2... 写order_dis表 orderid|分布大类|APPID|PlatForm - {分布小类：分布值}
		hbasekey = orderid + "|" + type1 + "|" + appid + "|" + platform;
		table.set(Bytes.toBytes(table_order_dis));
		put = new Put(Bytes.toBytes(hbasekey));
		put.add(cf, Bytes.toBytes(type2), Bytes.toBytes(counts + ""));
		context.write(table, put);
	}

	public String join(Iterable<Text> values, String separator) {
		if (!values.iterator().hasNext())
			return "";
		StringBuilder sb = new StringBuilder();
		Iterator<Text> i = values.iterator();
		sb.append(i.next().toString());
		while (i.hasNext()) {
			sb.append(separator);
			sb.append(i.next().toString());
		}
		return sb.toString();
	}
}
