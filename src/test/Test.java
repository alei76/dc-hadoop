package test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.common.WHDataUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.qq.jutil.string.RandomUtil;
import com.xunlei.util.Log;

public class Test {

	public static JsonParser jsonParser = new JsonParser();

	private static Logger logger = Log.getLogger("Test");

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException,
			SQLException {
		Test test = new Test();
		File f = new File(Test.class.getResource("").getPath());
		System.out.println(f);
		
		f = new File(Test.class.getResource("/").getPath());
		System.out.println(f);
	}

	public void testInteger() {
		int a = 1419955682;
		// System.out.println(a * 1000);
		int i = 1;
		int bitMove = i << 31;
		System.out.println(bitMove);
		System.out.println(bitMove | 1);
		System.out.println(Integer.MAX_VALUE);
		System.out.println(Integer.MIN_VALUE);
		// Calendar cal = Calendar.getInstance();
	}

	public void iteratorAndRemove(final ConcurrentHashMap<String, AtomicInteger> map) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Iterator<Entry<String, AtomicInteger>> iter = map.entrySet().iterator();
						while (iter.hasNext()) {
							String key = iter.next().getKey();
							int value = iter.next().getValue().intValue();
							iter.remove();
							System.out.println("key:" + key + ",value:" + value);
							Thread.sleep(10);
						}
						Thread.sleep(1000);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
		putAndUpdate(map);

	}

	public void putAndUpdate(final ConcurrentHashMap<String, AtomicInteger> map) {
		for (int i = 0; i < 10; i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					for (int j = 0; j < 100; j++) {
						String key = RandomUtil.getString("AB", 1);
						map.putIfAbsent(key, new AtomicInteger(0));
						AtomicInteger val = map.get(key);
						System.out.println(key + ":" + val.addAndGet(1));
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

			}).start();
			;
		}
	}

	public void readUTF() throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream("d:\\tmp.txt"));
		byte[] itemBuf = new byte[20];
		dis.read(itemBuf, 0, 6);
		String name = new String(itemBuf, 0, 6, "UTF-8");
		System.out.println(name);
	}

	public void readANSI() throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream("d:\\tmp2.txt"));
		byte[] itemBuf = new byte[20];
		dis.read(itemBuf, 0, 4);
		String name = new String(itemBuf, 0, 4, "GBK");
		System.out.println(name);
	}

	public void getEnv() {
		System.out.println(System.getenv());
		System.out.println("--------------------------------------------------");
		System.out.println(System.getProperties());
	}

	public void hiveJdbc() throws ClassNotFoundException, SQLException {
		String sql = "show create table payment";
		String sql2 = "select * from payment limit 5";
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection conn = DriverManager.getConnection("jdbc:hive2://119.147.212.248:10000/default", "hadoop", "");
		Statement stmt = conn.createStatement();
		ResultSet res = stmt.executeQuery(sql2);
		System.out.println(">>show result");
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}

	public void action1() {
		String info = "123	456		"; // 为么多个\t只能算分割了两个
		System.out.println(info.split("	").length);
	}

	/**
	 * <pre>
	 * 将Json格式字符串转换为Map对象
	 * 这里只解析如{"age":123,"name":"sam"}这类简单的Json串
	 * 暂不支持JSON为列表，对象的情况
	 * @author sam.xie
	 * @date 2015年3月2日 下午8:26:05
	 */
	public static Map<String, String> convertJson2Map(String json) {
		Map<String, String> map = new LinkedHashMap<String, String>();
		if (StringUtils.isBlank(json)) {
			System.out.println("json is null" + json);
			return map;
		}
		try {
			JsonObject jsonObj = (JsonObject) jsonParser.parse(json);
			for (Entry<String, JsonElement> entry : jsonObj.entrySet()) {
				String key = entry.getKey();
				JsonElement element = entry.getValue();
				if (element.isJsonPrimitive()) {
					map.put(key, element.getAsString());
				}
			}
		} catch (Exception e) {
			logger.error("Json Syntax Error");

		}
		return map;
	}

	public static void getDate() {
		Calendar cal = Calendar.getInstance();
		System.out.println(cal.getTimeInMillis());
		System.out.println(cal.getTime());
		cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1);
		cal.set(Calendar.HOUR, cal.get(Calendar.HOUR) - 14);

		logger.info(cal.getTime().toString());
	}

	public static void printSet(Set<String> set) {
		System.out.println("------------------------------------------------------------" + set.size());
		for (String item : set) {
			System.out.println(item);
		}
	}

	public static void testBitOperation() {
		int track = 3;
		boolean isLogin = true;
		int afterLogin = (track << 1) | (isLogin ? 1 : 0);
		System.out.println("after login：" + afterLogin);
		int days = 0;
		System.out.println("track >> days：" + (track >> days));
		int checkLogin = (track >> days) & 1;
		System.out.println("check result：" + checkLogin);
		byte i = -128;
	}

}
