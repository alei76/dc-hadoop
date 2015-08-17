package net.digitcube.hadoop.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.digitcube.hadoop.mapreduce.warehouse.common.AppModel;

public class JdbcUtil {

	public final static String jdbcUrl = "jdbc:mysql://192.168.1.175:3306/test?characterEncoding=utf-8&amp;autoReconnect=true";
	public final static String user = "dbuser";
	public final static String password = "user#2013!";
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(jdbcUrl, user, password);
	}

	public static void finallyClose(AutoCloseable conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	final static String getPluginConfigSQL = "select appid from dc_business_user.dc_game_plugins where PluginID=? and Visible=1";

	public static Set<String> getPluginConfig(int pluginType) {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet resultSet = null;
		Set<String> result = new HashSet<String>();
		try {
			connection = getConnection();
			ps = connection.prepareStatement(getPluginConfigSQL);
			ps.setInt(1, pluginType);
			resultSet = ps.executeQuery();
			if (resultSet != null) {
				while (resultSet.next()) {
					String appid = resultSet.getString(1);
					result.add(appid);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			finallyClose(resultSet);
			finallyClose(ps);
			finallyClose(connection);
		}
		return result;
	}
	
	final static String getAppBlacklistSQL = "select appid from de_warehouse.app_blacklist";
	public static Set<String> getAppBlacklist(){
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet resultSet = null;
		Set<String> result = new HashSet<String>();
		try {
			System.out.println("getAppBlacklistSQL:" + getAppBlacklistSQL);
			connection = getConnection();
			ps = connection.prepareStatement(getAppBlacklistSQL);
			resultSet = ps.executeQuery();
			if (resultSet != null) {
				System.out.println(">>>>>appBlacklist");
				while (resultSet.next()) {
					String appId = resultSet.getString(1);
					result.add(appId);
					System.out.println(appId);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			finallyClose(resultSet);
			finallyClose(ps);
			finallyClose(connection);
		}
		System.out.println(">>>>>appBlacklist size:" + result.size());
		return result;
	} 
	
	final static String getAppInfoSQL = "select t1.AppID, t1.Name, t1.Type, t2.TypeName, t4.EnName, t1.Flag, t1.Status, t1.CompanyId, t3.CompanyName, t1.CreateTime"
			+ " from dc_business_user.dc_games t1"
			+ " inner join dc_business_user.dc_game_type t2 on t1.Type = t2.Seqno"
			+ " inner join dc_business_user.dc_company t3 on t1.CompanyID = t3.Seqno"
			+ " inner join dc_business_user.dc_currency_type t4 on t1.CurrencyType = t4.Seqno";
	public static Map<String, AppModel> getAppInfoMap(){
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String, AppModel> result = new HashMap<String, AppModel>();
		try {
			System.out.println("getAppInfoSQL:" + getAppInfoSQL);
			connection = getConnection();
			ps = connection.prepareStatement(getAppInfoSQL);
			rs = ps.executeQuery();
			if (rs != null) {
				System.out.println(">>>>>appInfoMap");
				while (rs.next()) {
					AppModel app = new AppModel();
					String appId = rs.getString(1);
					result.put(appId, app);
					
					app.setAppId(rs.getString(1));
					app.setAppName(rs.getString(2));
					app.setTypeId(rs.getInt(3));
					app.setTypeName(rs.getString(4));
					app.setCurrency(rs.getString(5));
					app.setFlag(rs.getString(6));
					app.setStatus(rs.getInt(7));
					app.setCompanyId(rs.getInt(8));
					app.setCompanyName(rs.getString(9));
					app.setCreateTime(rs.getInt(10));
					System.out.println(app.toString());
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			finallyClose(rs);
			finallyClose(ps);
			finallyClose(connection);
		}
		System.out.println("appInfoMap size:" + result.size());
		return result;
	}
}
