package net.digitcube.hadoop.mapreduce.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseProxyClient {

	/**
	 * 插入一行记录
	 */
	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) {
		HConnection connection = null;
		HTableInterface table = null;
		try {
			connection = HbasePool.getConnection();
			table = connection.getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			HbasePool.close(table, connection);
		}
	}

	/**
	 * 删除一行记录
	 */
	public static void delRecord(String tableName, String rowKey)
			throws IOException {
		HConnection connection = null;
		HTableInterface table = null;
		try {
			connection = HbasePool.getConnection();
			table = connection.getTable(tableName);
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(rowKey.getBytes());
			list.add(del);
			table.delete(list);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			HbasePool.close(table, connection);
		}

	}

	/**
	 * 查找一行记录
	 */
	public static Result getOneRecord(HConnection connection, String tableName,
			String rowKey) throws IOException {
		HTableInterface table = null;
		try {
			table = connection.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			Result rs = table.get(get);
			return rs;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			HbasePool.close(table, null);
		}
	}

	/**
	 * 查找一行记录
	 */
	public static Result getOneRecord(String tableName, String rowKey)
			throws IOException {

		HConnection connection = null;
		HTableInterface table = null;
		try {
			connection = HbasePool.getConnection();
			table = connection.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			Result rs = table.get(get);
			return rs;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			HbasePool.close(table, connection);
		}
	}

	/**
	 * 查找某个 cf 的一行记录
	 */
	public static Result getCFOneRecord(String tableName, String rowKey, String columnFamily)
			throws IOException {

		HConnection connection = null;
		HTableInterface table = null;
		try {
			connection = HbasePool.getConnection();
			table = connection.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(Bytes.toBytes(columnFamily));
			Result rs = table.get(get);
			return rs;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			HbasePool.close(table, connection);
		}
	}
}
