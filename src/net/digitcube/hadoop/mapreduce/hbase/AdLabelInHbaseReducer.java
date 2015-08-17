package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.util.Iterator;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdLabelInHbaseReducer extends
		Reducer<OutFieldsBaseModel, Text, ImmutableBytesWritable, Put> {

	private static final byte[] cf = Bytes.toBytes("info");
	private ImmutableBytesWritable table = new ImmutableBytesWritable();

	public void reduce(OutFieldsBaseModel key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		String[] keyFields = key.getOutFields();
		String uid = keyFields[0];
		String platform = keyFields[1];
		String appid = keyFields[2];
		String table_order_accounts = context.getConfiguration().get(
				"hbase.tablename.ad_label");
		//
		String hbasekey = uid + "|" + platform;

		table.set(Bytes.toBytes(table_order_accounts));
		Put put = new Put(Bytes.toBytes(hbasekey));
		Iterator<Text> i = values.iterator();
		while (i.hasNext()) {
			put.add(cf, Bytes.toBytes(appid),
					Bytes.toBytes(i.next().toString()));
		}
		context.write(table, put);
	}
}
