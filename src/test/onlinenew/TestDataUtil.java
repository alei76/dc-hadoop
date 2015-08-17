package test.onlinenew;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;

public class TestDataUtil {

	/**
	 * 玩家在线日志格式：
	 * Timestamp,APPID,UID,AccountID,Platform,Channel,AccountType,Gender,Age,GameServer,
	 * Resolution,OperSystem,Brand,NetType,Country,Province,Operators,LoginTime,OnlineTime,Level
	 */
	public static List<String> generateOnlineData(int totalRecord, int loop, int baseMode){
		List<String> dataList = new ArrayList<String>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		StringBuilder sb = new StringBuilder();
		
		Calendar cal = Calendar.getInstance();
		//设置为前一小时
		cal.add(Calendar.HOUR_OF_DAY, -1);
		//分、秒置 0
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		
		for(int i=1; i<=totalRecord; i++){
			cal.add(Calendar.MINUTE, 5);
			//cal.add(Calendar.MINUTE, 1);
			//cal.add(Calendar.SECOND, 5);
			int loginTime = (int)(cal.getTimeInMillis()/1000);
			int mode = i%baseMode;
			for(int j=1; j<=loop; j++){
				
				sb.delete(0, sb.length());
				sb.append("ts").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("appid_).append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("appid").append(MRConstants.SEPERATOR_IN);
				sb.append("uid").append(MRConstants.SEPERATOR_IN);
				sb.append("accId").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("plat").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("plat").append(MRConstants.SEPERATOR_IN);
				//sb.append("chan").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("chan").append(MRConstants.SEPERATOR_IN);
				sb.append("accTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gend").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("age").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("gServ").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gServ").append(MRConstants.SEPERATOR_IN);
				sb.append("resol").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("opSys").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("brand").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("netTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("cntry").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("provn").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("oper").append(i).append(MRConstants.SEPERATOR_IN);
				
				int factor = i > totalRecord/2 ? (totalRecord - i + 1) : i;
				int onlineTime = 60 * j * factor;
				//int onlineTime = 5 * 2;
				int level1 = j + i;
				
				sb.append(loginTime).append(MRConstants.SEPERATOR_IN);
				//sb.append(sdf.format(cal.getTime())).append(MRConstants.SEPERATOR_IN);
				sb.append(onlineTime).append(MRConstants.SEPERATOR_IN);
				sb.append(level1).append(MRConstants.SEPERATOR_IN);
				
				sb.delete(sb.length() - MRConstants.SEPERATOR_IN.length(), sb.length());
				dataList.add(sb.toString());
				
				
				//generate for union
				/*sb.delete(0, sb.length());
				sb.append("ts").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("appid_).append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("appid").append(mode).append(MRConstants.SEPERATOR_IN);
				sb.append("uid").append(mode).append(MRConstants.SEPERATOR_IN);
				sb.append("accId").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("plat").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("plat").append(MRConstants.SEPERATOR_IN);
				//sb.append("chan").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("chan").append(MRConstants.SEPERATOR_IN);
				sb.append("accTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gend").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("age").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("gServ").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gServ").append(MRConstants.SEPERATOR_IN);
				sb.append("resol").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("opSys").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("brand").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("netTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("cntry").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("provn").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("oper").append(i).append(MRConstants.SEPERATOR_IN);
				
				sb.append(loginTime).append(MRConstants.SEPERATOR_IN);
				//sb.append(sdf.format(cal.getTime())).append(MRConstants.SEPERATOR_IN);
				sb.append(onlineTime).append(MRConstants.SEPERATOR_IN);
				sb.append(level1).append(MRConstants.SEPERATOR_IN);
				
				sb.delete(sb.length() - MRConstants.SEPERATOR_IN.length(), sb.length());
				dataList.add(sb.toString());*/
			}
			
		}
		return dataList;
	}
	
	/**
	 * 玩家激活注册日志格式：
	 * Timestamp,APPID,UID,AccountID,Platform,Channel,AccountType,Gender,Age,GameServer,
	 * Resolution,OperSystem,Brand,NetType,Country,Province,Operators,ActTime,RegTime
	 */
	public static List<String> generateUserInfoData(int totalRecord, int baseMode){
		List<String> dataList = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		
		for(int i=1; i<=totalRecord; i++){
			
				sb.delete(0, sb.length());
				sb.append("ts").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("appid_).append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("appid").append(MRConstants.SEPERATOR_IN);
				sb.append("uid").append(MRConstants.SEPERATOR_IN);
				sb.append("accId").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("plat").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("plat").append(MRConstants.SEPERATOR_IN);
				//sb.append("chan").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("chan").append(MRConstants.SEPERATOR_IN);
				sb.append("accTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gend").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("age").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("gServ").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gServ").append(MRConstants.SEPERATOR_IN);
				sb.append("resol").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("opSys").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("brand").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("netTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("cntry").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("provn").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("oper").append(i).append(MRConstants.SEPERATOR_IN);
				
				String actTime = "" + System.currentTimeMillis()/1000;
				String regTime = "-";
				if(0 == i%3){
					regTime = actTime; 
				}
				sb.append(actTime).append(MRConstants.SEPERATOR_IN);
				sb.append(regTime).append(MRConstants.SEPERATOR_IN);				
				sb.append(i).append(MRConstants.SEPERATOR_IN);
				
				sb.delete(sb.length() - MRConstants.SEPERATOR_IN.length(), sb.length());
				dataList.add(sb.toString());
				
				//generate for union
				/*int mode = i%baseMode;
				sb.delete(0, sb.length());
				sb.append("ts").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("appid_).append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("appid").append(mode).append(MRConstants.SEPERATOR_IN);
				sb.append("uid").append(mode).append(MRConstants.SEPERATOR_IN);
				sb.append("accId").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("plat").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("plat").append(MRConstants.SEPERATOR_IN);
				//sb.append("chan").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("chan").append(MRConstants.SEPERATOR_IN);
				sb.append("accTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gend").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("age").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("gServ").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gServ").append(MRConstants.SEPERATOR_IN);
				sb.append("resol").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("opSys").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("brand").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("netTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("cntry").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("provn").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("oper").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append(actTime).append(MRConstants.SEPERATOR_IN);
				sb.append(regTime).append(MRConstants.SEPERATOR_IN);				
				
				sb.delete(sb.length() - MRConstants.SEPERATOR_IN.length(), sb.length());
				dataList.add(sb.toString());*/
		}
		return dataList;
	}
	
	/**
	 * 新增玩家付费
	 * @param totalRecord
	 * @param baseMode
	 * @return
	 */
	public static List<String> generatePayData(int totalRecord, int baseMode){
		List<String> dataList = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR_OF_DAY, -1);
		int payTime = (int)(cal.getTimeInMillis()/1000);
		for(int i=1; i<=totalRecord; i++){
			
				//int mode = i%baseMode;
				sb.delete(0, sb.length());
				sb.append("ts").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("appid_).append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("appid").append(MRConstants.SEPERATOR_IN);
				sb.append("uid").append(MRConstants.SEPERATOR_IN);
				sb.append("accId").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("plat").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("plat").append(MRConstants.SEPERATOR_IN);
				//sb.append("chan").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("chan").append(MRConstants.SEPERATOR_IN);
				sb.append("accTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gend").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("age").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("gServ").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gServ").append(MRConstants.SEPERATOR_IN);
				sb.append("resol").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("opSys").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("brand").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("netTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("cntry").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("provn").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("oper").append(i).append(MRConstants.SEPERATOR_IN);
				
				//充值金额
				sb.append(i).append(MRConstants.SEPERATOR_IN);
				//虚拟金币
				sb.append(i*10).append(MRConstants.SEPERATOR_IN);
				sb.append(i+i).append(MRConstants.SEPERATOR_IN);
				//充值货币
				sb.append("rmb").append(MRConstants.SEPERATOR_IN);
				//充值渠道
				sb.append("iphone").append(MRConstants.SEPERATOR_IN);
				//等级
				sb.append(i-1).append(MRConstants.SEPERATOR_IN);
				sb.append(i-1).append(MRConstants.SEPERATOR_IN);
				sb.append(payTime).append(MRConstants.SEPERATOR_IN);
				
				sb.delete(sb.length() - MRConstants.SEPERATOR_IN.length(), sb.length());
				dataList.add(sb.toString());
		}
		return dataList;
	}
	
	/**
	 * 产生事件数据
	 * @param totalRecord
	 * @param baseMode
	 * @return
	 */
	public static List<String> generateEventData(int totalRecord, int baseMode){
		List<String> dataList = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		
		for(int i=1; i<=totalRecord; i++){
			
				sb.delete(0, sb.length());
				sb.append("ts").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("appid_).append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("appid").append(MRConstants.SEPERATOR_IN);
				sb.append("uid"+i).append(MRConstants.SEPERATOR_IN);
				sb.append("accId").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("plat").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("plat").append(MRConstants.SEPERATOR_IN);
				//sb.append("chan").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("chan").append(MRConstants.SEPERATOR_IN);
				sb.append("accTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gend").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("age").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("gServ").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gServ").append(MRConstants.SEPERATOR_IN);
				sb.append("resol").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("opSys").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("brand").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("netTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("cntry").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("provn").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("oper").append(i).append(MRConstants.SEPERATOR_IN);
				
				String eventID = "id"+i%3;
				if(0 == i%3){
					eventID = Constants.EVENT_ID_LEVEL_UP + ":" + i; 
				}
				sb.append(eventID).append(MRConstants.SEPERATOR_IN);
				
				int duration = i * 1000;
				sb.append(duration).append(MRConstants.SEPERATOR_IN);
				
				if(eventID.contains(Constants.EVENT_ID_LEVEL_UP)){
					String eventAttr = "-";
					sb.append(eventAttr).append(MRConstants.SEPERATOR_IN);
				}else{
					String eventAttr = "a:"+"1a,b:"+"1b";
					sb.append(eventAttr).append(MRConstants.SEPERATOR_IN);
				}
				
				sb.delete(sb.length() - MRConstants.SEPERATOR_IN.length(), sb.length());
				dataList.add(sb.toString());
				
				//generate for union
				/*int mode = i%baseMode;
				sb.delete(0, sb.length());
				sb.append("ts").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("appid_).append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("appid").append(mode).append(MRConstants.SEPERATOR_IN);
				sb.append("uid").append(mode).append(MRConstants.SEPERATOR_IN);
				sb.append("accId").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("plat").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("plat").append(MRConstants.SEPERATOR_IN);
				//sb.append("chan").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("chan").append(MRConstants.SEPERATOR_IN);
				sb.append("accTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gend").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("age").append(i).append(MRConstants.SEPERATOR_IN);
				//sb.append("gServ").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("gServ").append(MRConstants.SEPERATOR_IN);
				sb.append("resol").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("opSys").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("brand").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("netTp").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("cntry").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("provn").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append("oper").append(i).append(MRConstants.SEPERATOR_IN);
				sb.append(actTime).append(MRConstants.SEPERATOR_IN);
				sb.append(regTime).append(MRConstants.SEPERATOR_IN);				
				
				sb.delete(sb.length() - MRConstants.SEPERATOR_IN.length(), sb.length());
				dataList.add(sb.toString());*/
		}
		return dataList;
	}
	
	public static void main(String[] args){
		List<String> list = generatePayData(10,1);
		for(String s : list){
			System.out.println(s);
		}
	}
}
