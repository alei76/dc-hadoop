package net.digitcube.hadoop.mapreduce.hbase.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HbasePool {

	private static String hbase_zookeeper_quorum = "dcnamenode1,dchbase1,dchbase2";
	private static String hbase_zookeeper_property_clientPort = "2181";
	private static Configuration configuration;

	private static HConnection hConnection = null;

	public static HConnection getConnection() throws ZooKeeperConnectionException {
		if (hConnection == null) {
			synchronized (HbasePool.class) {
				if (hConnection == null) {
					configuration = HBaseConfiguration.create();
					configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
					configuration.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_property_clientPort);
					
					hConnection = HConnectionManager.createConnection(configuration);
				}
			}
		}
		// return HConnectionManager.getConnection(configuration);
		return hConnection;
	}

	public static void closeAllConns(){
		try {
			if (hConnection != null)
				hConnection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void close(HTableInterface table) {
		try {
			if (table != null)
				table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void close(HConnection connection) {
		try {
			if (connection != null)
				connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void close(HTableInterface table, HConnection connection) {
		try {
			if (table != null)
				table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			if (connection != null)
				connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
