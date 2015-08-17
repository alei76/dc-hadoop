package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;
import java.util.Calendar;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.DateUtil;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.qq.jutil.crypto.TEACoding;

/**
  * 
 * 主要逻辑：
 * 对每玩家当天的 PV 进行去重汇总，输出每玩家的 PV 总数以及 PV 为 1 时的次数
 * 
 * 
 * 输入：EventLog(H5 自定义时间 DC_PV 的日志)
 * 
 * map：
 * key = appId, appVersion, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, accountId
 * val = isNewPlayer, pvKey(page + "|" + optime)
 * 
 * reduce：
 * for val : values
 * do
 *     if isNewPlayer
 *         isNewPlayer = true
 *     end if
 * 
 *     oldCount = pvMap.get(pvKey)
 *     pvMap.put( pvKey , oldCount＋ 1)
 * done
 * 
 * playerTotalPVs = 0
 * player1ViewPvs = 0
 * playerPVRecords = ""
 * for entry : pvMap
 * do
 *     playerTotalPVs += entry.getValue
 *     if (1 == entry.getValue)
 *         player1ViewPvs++
 *     enf if
 * 
 *     playerPVRecords += entry.getKey + entry.getValue
 * done
 * 
 * 输出：
 * appId, appVersion, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, accountId, 
 * isNewPlayer, totalLoginTimes, totalOnlineTime, totalIpCount, onlineRecords, playerIpSet
 * 
 */

public class H5PageViewForPlayerMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	private OutFieldsBaseModel mapValObj = new OutFieldsBaseModel();
	
	private Calendar cal = Calendar.getInstance();
	private int statDate = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = DateUtil.getStatDateForHourOrToday(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] eventArr = value.toString().split(MRConstants.SEPERATOR_IN);
		EventLog eventLog = null;
		try{
			//日志里某些字段有换行字符，导致这条日志换行而不完整，
			//用 try-catch 过滤掉这类型的错误日志
			eventLog = new EventLog(eventArr);
		}catch(Exception e){
			//TODO do something to mark the error here
			return;
		}
		
		String H5_PromotionAPP = eventLog.getH5PromotionApp();
		String H5_DOMAIN = eventLog.getH5Domain();
		String H5_REF = eventLog.getH5Refer();
		
		/*if(eventLog.isReferDomainEmpty() 
				|| StringUtil.isEmpty(eventLog.getUID())
				|| 66 != eventLog.getUID().length()){
			return;
		}*/
		//20150528:白鹭接入UID不是 66 位长度，不包含创建时间
		if(eventLog.isReferDomainEmpty() 
				|| StringUtil.isEmpty(eventLog.getUID())){
			return;
		}
		
		String isNewPlayer = statDate == parseForNewAddDate(eventLog.getUID()) 
							   	? Constants.DATA_FLAG_YES
								: Constants.DATA_FLAG_NO;
		String page = eventLog.getArrtMap().get("page");
		String openTime = eventLog.getArrtMap().get("optime");
		String loginTime = eventLog.getArrtMap().get("lttime");
		
		if(StringUtil.isEmpty(openTime) || StringUtil.isEmpty(loginTime)){
			return;
		}
		
		String[] keyFields = new String[] {
				eventLog.getAppID(),
				eventLog.getPlatform(),
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
				eventLog.getAccountID()
		};
		
		//每次 PV 以 页面名称和打开时间作为标记
		String pvKey = page + "|" + openTime; 
		String[] valFields = new String[]{
				isNewPlayer,
				loginTime,
				pvKey
		};
		
		mapKeyObj.setOutFields(keyFields);
		mapValObj.setOutFields(valFields);
		context.write(mapKeyObj, mapValObj);
	}
	
	/**
	 * 从 UID 中解析得到玩家的新增时间，66 UID 信息如下：
	 * QZF665CBE07F24C30C8F57252AE804C3F3153E0F42AB620A8EB0EE931BB131D4F8
	 * 前两位 QZ|QQ|WX 是应用平台，可以直接截取掉
	 * 后面64位使用Tea加密，秘钥是 : tencent#X@~g()[]
	 * 解开后是16byte的数组，由两个long型组成
	 * 第一个long是生成时间戳，到毫秒, 第二个long是IP地址
	 */
	private  int parseForNewAddDate(String uid){
		//Added at 20150528
		//白鹭接入的 UID 格式已经更改，及 UID 中不再包寒玩家新增时间
		//为快速兼容，碰到非 66 位 UID 时，直接返回 0，即不视为新增玩家判断
		//后期新增玩家需通过滚存判断
		if(StringUtil.isEmpty(uid) || 66 != uid.length()){
			return 0;
		}
		try{
			//原始 UID 中前面两位是推广平台，后面 64 位是真实 UID
			String uid2 = uid.substring(2);
			byte[] b = new TEACoding("tencent#X@~g()[]".getBytes()).decodeFromHexStr(uid2);
			long l = Bytes.toLong(b, 0, 8);
			cal.setTimeInMillis(l);
			
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			return (int) (cal.getTimeInMillis() / 1000);
			
		}catch(Throwable t){
			return 0;
		}
	}
	
	public static void main(String[] args) {
		String uid = "QQDD8813FDF0D5A339E92B698075470B4037B6DF21FD667C07DB9A95632CC72B66";
		H5PageViewForPlayerMapper a = new H5PageViewForPlayerMapper();
		System.out.println(a.parseForNewAddDate(uid));//1414339200
	}
	
}

