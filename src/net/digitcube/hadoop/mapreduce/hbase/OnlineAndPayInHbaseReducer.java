package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OnlineAndPayInHbaseReducer extends Reducer<OutFieldsBaseModel, Text, ImmutableBytesWritable, Put> {
	private static final byte[] cf = Bytes.toBytes("info");
	private static final byte[] qualifier = Bytes.toBytes("q");
	private StringBuilder sb = new StringBuilder();
	private ImmutableBytesWritable table = new ImmutableBytesWritable();
	
	public void reduce(OutFieldsBaseModel key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//keyBytes
		sb.delete(0, sb.length());
		for (String val : key.getOutFields()) {
			sb.append(val.toString()).append("|");
		}
		sb.deleteCharAt(sb.length() - 1);
		byte[] keyBytes = Bytes.toBytes(sb.toString());
		
		//valBytes
		sb.delete(0, sb.length());
		for (Text val : values) {
			sb.append(val.toString()).append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		byte[] valBytes = Bytes.toBytes(sb.toString());
		
		//target table
		String tableName = key.getSuffix();
		table.set(Bytes.toBytes(tableName));
		
		//current row
		Put put = new Put(keyBytes);
		put.add(cf, qualifier, valBytes);
		
		//write to table
		context.write(table, put);
	}
}
