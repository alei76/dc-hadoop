package test.gendata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * 该类主要用于产生测试数据
 * 数据包括：
 * a) 激活、注册数据
 * b) 在线、首登数据
 * c) 付费、首付数据
 * 
 * 产生规则里包括活跃玩家付费玩家的新增、流失和回流
 *
 */
public class TestDataGenerator {

	private static String[] appid = {"APPID1","APPID1"};
	private static String[] platform = {"1","2"};
	private static String[] channel = {"ch1","ch2"};
	private static String[] gameserver = {"gs1","gs2"};
	private static String[] accountType = {"qq","sina"};
	private static String[] province = {"guangdong","shanghai"};
	private static String[] operator = {"liantong","dianxin"};
	private static String[] nettype = {"1","2"};
	private static String[] brand = {"iphone","samsung"};
	private static String[] os = {"ios","android"};
	private static String[] rel = {"1024*768","800*600"};
	private static String[] gender = {"1","2"};
	private static String[] age = {"11","16","21","26","31","36","41","46","51","56","61"};
	
	private static String LOG_BASIC_DIR = "/home/hadoop/rickpan/logserver";
	private static final String LOG_USERINFO = "userinfo";
	private static final String LOG_ONLINE = "online";
	private static final String LOG_ONLINE_FIRST = "online_first";
	private static final String LOG_PAYMENT = "payment";
	private static final String LOG_PAYMENT_FIRST = "payment_first";
	
	private static int DAYS = 45;
	
	private static StringBuilder buf = new StringBuilder(); 
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
	private static SimpleDateFormat sdfFileName = new SimpleDateFormat("yyyyMMddHH");
	private static Set<UserModel> userSet = new HashSet<UserModel>();
	
	public static void main(String[] args) throws IOException {
		if(args.length < 2){
			System.out.println("Command Error. Usage:");
			System.out.println("java -jar genTestData.jar baseDir days");
			System.exit(1);
		}
		
		LOG_BASIC_DIR = args[0];
		DAYS = Integer.valueOf(args[1]);
		
		System.out.println("Generating data...");
		generateData();
		System.out.println("End generating data...");
	}

	/**
	 * 从给定的日期开始，产生 60 天的数据
	 * 
	 * 在 21 天之前，分别以第 7,14,21  天为时间点，在每天每小时的每个 5 分钟内
	 * 产生 2 个新玩家，其中一个为付费玩家
	 * 
	 * 每个玩家特性如下：
	 * 天数小于 7 时，新玩家在线周期为 7 天（在线情况为奇数周期在线，偶数周期不在线）
	 * （即第一个 7 天在线，第二个 7 天不在线，第三个 7 天在线，第四个 7 天不在线...）
	 * 付费开始事件为 7/2=3，付费规律为第一个在线周期开始付费后，后面每个在线周期都会付费
	 * 
	 * 天数小于 14、21 在线和付费计算规则与小于 7 时相同
	 * 
	 * 
	 * 
	 * 从 21 天开始往后，不在产生新玩家，并且原有的所有玩家也不再付费
	 * 同时玩家流失规则设定如下：
	 * 天数在 21~42 天时，在线周期为 7 的玩家流失
	 * 天数在 43~60 天时，在线周期为 7 和 14 的玩家流失
	 * 
	 * @param c
	 * @throws IOException 
	 */
	private static void generateData() throws IOException{
		
		Calendar c = Calendar.getInstance();
		c.set(Calendar.MONTH, 6);
		c.set(Calendar.DAY_OF_MONTH, 0);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		
		for(int i=1; i<=DAYS; i++){
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.add(Calendar.DAY_OF_MONTH, 1);

			System.out.println(sdf.format(new Date(System.currentTimeMillis())) + "-->"+sdf.format(c.getTime()));
			
			if(i<=21){ // 小于等 21 天，每天产生新玩家
				
				for(int j=0; j<24; j++){
					c.set(Calendar.HOUR_OF_DAY, j);
					c.set(Calendar.MINUTE, 0);
					
					//1, 注册激活信息
					BufferedWriter userInfoWriter = getWriteFile(LOG_BASIC_DIR, LOG_USERINFO, c);
					
					//2, 在线首登信息
					BufferedWriter onlineWriter = getWriteFile(LOG_BASIC_DIR, LOG_ONLINE, c);
					BufferedWriter onlineFirstWriter = getWriteFile(LOG_BASIC_DIR, LOG_ONLINE_FIRST, c);
					
					//3, 付费和首付信息
					BufferedWriter payWriter = getWriteFile(LOG_BASIC_DIR, LOG_PAYMENT, c);
					BufferedWriter payFirstWriter = getWriteFile(LOG_BASIC_DIR, LOG_PAYMENT_FIRST, c);
					
					for(int k=0; k<12; k++){
						c.set(Calendar.MINUTE, 1 + k*5);

						//5的时候数据量太大，测试机器跑不动
						//for(int m=0; m<5; m++){
						for(int m=0; m<2; m++){
							UserModel u = new UserModel();
							u.uid = UUID.randomUUID().toString(); // 随机 uid
							u.aid = u.uid;
							u.bornDate = (int)(c.getTimeInMillis()/1000); // 出生时间
							if(i<=7){
								u.onlineInterval = 7;  //在线周期 7 天
							}else if(i<=14){
								u.onlineInterval = 14;  //在线周期 14 天
							}else if(i<=21){
								u.onlineInterval = 21;  //在线周期 21/ 天
							}
							
							u.firstPayDays = u.onlineInterval/2;//在线周期除以 2
							
							u.onlineTime = 60; // 60s
							if(0 == m%2){
								u.isPayPlayer = true;
							}
							userSet.add(u);
							
							//1, 注册激活
							logUserInfo(userInfoWriter, u, c);
						}
						
						//2, 在线和首登，测试计算 ACU/PCU
						logOnlineInfo(onlineWriter, onlineFirstWriter, c);
					}
					
					//3, 付费和首付，每小时打印一次日志
					logPaymentInfo(payWriter, payFirstWriter, c);
					
					//本地测试，忽略关闭异常的情况
					userInfoWriter.close();
					
					onlineWriter.close();
					onlineFirstWriter.close();
					
					payWriter.close();
					payFirstWriter.close();
				}
				
			} else if(i<=42){
				// 大于 21  小于等 42 天，不再产生新玩家，并且原有 7 天在线周期 玩家也将会流失
				for(int j=0; j<24; j++){
					c.set(Calendar.HOUR_OF_DAY, j);
					c.set(Calendar.MINUTE, 0);
					
					//1, 在线信息(不再有首登)
					BufferedWriter onlineWriter = getWriteFile(LOG_BASIC_DIR, LOG_ONLINE, c);
					
					for(int k=0; k<12; k++){
						c.set(Calendar.MINUTE, 1 + k*5);
						for(int m=0; m<5; m++){
							//2, 在线，为计算 ACU/PCU，所以每 5 分钟打一次日志
							logLostOnlineInfo(onlineWriter, c, 7);
						}
					}
					
					//本地测试，忽略关闭异常的情况
					onlineWriter.close();
				}
				
			} else{
				// 大于  42 天，不再产生新玩家，并且原有 7,14  天在线周期玩家也视为流失
				for(int j=0; j<24; j++){
					c.set(Calendar.HOUR_OF_DAY, j);
					c.set(Calendar.MINUTE, 0);
					
					//1, 在线信息(不再有首登)
					BufferedWriter onlineWriter = getWriteFile(LOG_BASIC_DIR, LOG_ONLINE, c);
					
					for(int k=0; k<12; k++){
						c.set(Calendar.MINUTE, 1 + k*5);
						for(int m=0; m<5; m++){
							//2, 在线，为计算 ACU/PCU，所以每 5 分钟打一次日志
							logLostOnlineInfo(onlineWriter, c, 14);
						}
					}
					
					//本地测试，忽略关闭异常的情况
					onlineWriter.close();
				}
			}
		}
	}
	
	/**
	 * 打印激活登录信息
	 * 
	 * @param writer
	 * @param u
	 * @param c
	 * @throws IOException
	 */
	private static void logUserInfo(BufferedWriter writer, UserModel u, Calendar c) throws IOException{
		buf.delete(0, buf.length());
		setCommonHeader(buf, u, c);
		int actTime = (int)(c.getTimeInMillis()/1000);
		int regTime = (int)(c.getTimeInMillis()/1000);
		buf.append(actTime).append("\t")
		   .append(regTime);
		
		writer.write(buf.toString());
		writer.newLine();
	}
	
	/**
	 * 打印在线和首登日志
	 * 
	 * @param onlineWriter
	 * @param onlineFirstWriter
	 * @param c
	 * @throws IOException
	 */
	private static void logOnlineInfo(BufferedWriter onlineWriter, BufferedWriter onlineFirstWriter, Calendar c) throws IOException{
		
		for(UserModel u : userSet){
			
			if(isPlayerOnlineNow(u, c)){
				buf.delete(0, buf.length());
				setCommonHeader(buf, u, c);
				int logintime = (int)(c.getTimeInMillis()/1000);
				int onlinetime = u.onlineTime;
				int level = u.actualOnlineDays;
				
				buf.append(logintime).append("\t")
				   .append(onlinetime).append("\t")
				   .append(level);
				
				onlineWriter.write(buf.toString());
				onlineWriter.newLine();
				if(!u.isLogin){
					onlineFirstWriter.write(buf.toString());
					onlineFirstWriter.newLine();
					u.isLogin = true;
				}
			}
		}
	}
	
	/**
	 * 小于某个在线周期的玩家过滤掉
	 * 如 lostInterval 为 7 时，在线周期为 7 的玩家被过滤掉
	 * 如 lostInterval 为 14 时，在线周期为 7 和 14 的玩家被过滤掉
	 * 
	 * @param onlineWriter
	 * @param c
	 * @param lostInterval
	 * @throws IOException
	 */
	private static void logLostOnlineInfo(BufferedWriter onlineWriter, Calendar c, int lostInterval) throws IOException{
		for(UserModel u : userSet){
			
			if(u.onlineInterval <= lostInterval){
				continue;
			}
			if(isPlayerOnlineNow(u, c)){
				buf.delete(0, buf.length());
				setCommonHeader(buf, u, c);
				int logintime = (int)(c.getTimeInMillis()/1000);
				int onlinetime = u.onlineTime;
				int level = u.actualOnlineDays;
				
				buf.append(logintime).append("\t")
				   .append(onlinetime).append("\t")
				   .append(level);
				
				onlineWriter.write(buf.toString());
				onlineWriter.newLine();
			}
		}
	}
	
	/**
	 * 打印付费和首付日志
	 * 
	 * @param onlineWriter
	 * @param onlineFirstWriter
	 * @param c
	 * @throws IOException
	 */
	private static void logPaymentInfo(BufferedWriter payWriter, BufferedWriter payFirstWriter, Calendar c) throws IOException{
		for(UserModel u : userSet){
			
			if(u.isPayPlayer && isPlayerOnlineNow(u, c) && isPlayerStartPay(u, c)){
				buf.delete(0, buf.length());
				setCommonHeader(buf, u, c);
				
				int currencyAmount = 1;	// 真实货币，1 元? 1 角?
				int VirtualCurrencyAmount = 10; //虚拟货币，10 倍于真实货币
				String Iapid = "Iapid1";
				String CurrencyType = "RMB";
				String PayType = "bank";
				int level = u.actualOnlineDays;
				int PayTime = (int)(c.getTimeInMillis()/1000);
				buf.append(currencyAmount).append("\t")
				   .append(VirtualCurrencyAmount).append("\t")
				   .append(Iapid).append("\t")
				   .append(CurrencyType).append("\t")
				   .append(PayType).append("\t")
				   .append(level).append("\t")
				   .append(PayTime);
				
				payWriter.write(buf.toString());
				payWriter.newLine();
				
				if(!u.isPaied){
					payFirstWriter.write(buf.toString());
					payFirstWriter.newLine();
					u.isPaied = true;
				}
			}
		}
	}
	
	/**
	 * 玩家在线规则设定：
	 * 当前时间 减去 玩家出生时间所得到的天数
	 * 如果该天数落在该玩家 onlineInterval 的奇数周期内，则标识在线
	 * 反之落在该玩家 onlineInterval 的偶数周期内，则表示不在线
	 * 
	 * 如，某玩家的  onlineInterval 为 7
	 * 那么其奇数周期内日期有 ：[0,7),[14,21),..
	 * 那么其偶数周期内日期有 ：[7,14),[21,28),...
	 * 
	 * 所以当  当前时间 减去 玩家出生时间 = [0,1,2,3,4,5,6, 14,15,16,17,18,19,20...]时表示用户在线
	 * 所以当  当前时间 减去 玩家出生时间 = [7,8,9,10,11,12,13, 21,22,23,24,25,26,27...]时表示用户不在线
	 * 
	 * @param c
	 * @param u
	 * @return
	 */
	private static boolean isPlayerOnlineNow(UserModel u, Calendar c){
		int currentTime = (int)(c.getTimeInMillis()/1000);
		int daysElapse = (currentTime - u.bornDate)/60/60/24;
		u.actualOnlineDays = daysElapse; //玩家级别 = 玩家在线天数
		
		if(0 == ((daysElapse/u.onlineInterval)%2)){
			return true;
		}
		
		return false;
	}
	
	/**
	 * 玩家从 bornDate + u.firstPayDays 开始付费
	 * 
	 * @param u
	 * @param c
	 * @return
	 */
	private static boolean isPlayerStartPay(UserModel u, Calendar c){
		int currentTime = (int)(c.getTimeInMillis()/1000);
		int daysElapse = (currentTime - u.bornDate)/60/60/24;
		
		if(daysElapse >= u.firstPayDays){
			return true;
		}
		
		return false;
	}
	
	/**
	 * 检查某个时间点的目录是否存在，不存在则创建
	 * 
	 * @param baseDir
	 * @param componentName
	 * @param c
	 * @return
	 */
	private static File checkAndMkdir(String baseDir, String componentName, Calendar c){
		int year = c.get(Calendar.YEAR);
		int month = c.get(Calendar.MONTH)+1;
		int day = c.get(Calendar.DAY_OF_MONTH);
		int hour = c.get(Calendar.HOUR_OF_DAY);
		File dir = new File(baseDir + File.separator
							+ componentName + File.separator
							+ year + File.separator
							+ (month>=10 ? month : "0"+month) + File.separator
							+ (day>=10 ? day : "0"+day) + File.separator
							+ (hour>=10 ? hour : "0"+hour) + File.separator
							+ "input");
		if(!dir.exists()){
			dir.mkdirs();
		}
		
		return dir;
	}
	private static BufferedWriter getWriteFile(String baseDir, String componentName, Calendar c) throws IOException{
		File parentDir = checkAndMkdir(baseDir, componentName, c);
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(parentDir, componentName + "_" + sdfFileName.format(c.getTime()))));
		return writer;
	}
	
	/**
	 * 拼装日志公共头部
	 * @param buf
	 * @param u
	 * @param c
	 */
	private static void setCommonHeader(StringBuilder buf, UserModel u, Calendar c){
		int timestamp = (int)(c.getTimeInMillis()/1000);
		String apid = appid[0];
		String uid = u.uid;
		String aid = u.aid;
		String pf = platform[0];
		String ch = channel[0];
		String actype = accountType[0];
		String gend = gender[0];
		String ag = age[0];
		String gs = gameserver[0];
		String rl = rel[0];
		String osStr = os[0];
		String bd = brand[0];
		String nt = nettype[0];
		String country = "1";
		String prov = province[0];
		String op = operator[0];
		
		//buf.append(sdf.format(c.getTime())).append("\t");
		buf.append(timestamp).append("\t")
		   .append(apid).append("\t")
		   .append(uid).append("\t")
		   .append(aid).append("\t")
		   .append(pf).append("\t")
		   .append(ch).append("\t")
		   .append(actype).append("\t")
		   .append(gend).append("\t")
		   .append(ag).append("\t")
		   .append(gs).append("\t")
		   .append(rl).append("\t")
		   .append(osStr).append("\t")
		   .append(bd).append("\t")
		   .append(nt).append("\t")
		   .append(country).append("\t")
		   .append(prov).append("\t")
		   .append(op).append("\t");
	}
	
	
	static class UserModel{
		String uid = "";
		String aid = "";
		
		//出生日期
		int bornDate = 0;
		
		//首付时间
		int firstPayDays = 0;
		
		//在线周期，奇数周期为在线，偶数周期为下线
		int onlineInterval = 0;
		
		//已经在线的时长
		int actualOnlineDays = 0;
		
		//在线时长（每次上报，每次默认 1 分钟）
		int onlineTime = 0;
		
		//是否是付费用户
		boolean isPayPlayer = false;
		
		//是否已经付过费（打印首付日志用）
		boolean isPaied = false;
		
		//是否已经登录过（打印首登日志用）
		boolean isLogin = false;
		
	}
}

