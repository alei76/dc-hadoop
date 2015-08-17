package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;
import java.util.Calendar;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.DateUtil;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.qq.jutil.crypto.TEACoding;

/**
 * 输入：@H5OnlineDayForPlayerMapper
 * 输出：
 * 新增玩家数：appId, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, Constants.PLAYER_TYPE_NEWADD, newPlayerNum
 * 30 日留存: appId, platform, H5_PromotionAPP, H5_DOMAIN, H5_REF, playerType, dayOffset, statPlayerNum
 */
public class H5Player30dayRetainMapper extends Mapper<LongWritable, Text, OutFieldsBaseModel, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	
	private Calendar cal = Calendar.getInstance();
	private int statDate = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statDate = DateUtil.getStatDate(context.getConfiguration());
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(MRConstants.SEPERATOR_IN);
		int i = 0;
		String appId = arr[i++];
		String platform = arr[i++];
		String H5_PromotionAPP = arr[i++];
		String H5_DOMAIN = arr[i++];
		String H5_REF = arr[i++];
		String accountId = arr[i++];
		String isNewPlayer = arr[i++];
		String totalLoginTimes = arr[i++];
		String totalOnlineTime = arr[i++];
		String uniqIpCount = arr[i++];
		String onlineRecords = arr[i++];
		String ipRecords = arr[i++];
		String uid = arr[i++];
		
		//新增玩家人数统计
		if(Constants.DATA_FLAG_YES.equals(isNewPlayer)){
			String[] keyFields = new String[] { 
					appId,
					platform,
					H5_PromotionAPP,
					H5_DOMAIN,
					H5_REF,
					Constants.PLAYER_TYPE_NEWADD
			};
			
			mapKeyObj.setSuffix(Constants.SUFFIX_H5_NEWADD_DAY_SUM);
			mapKeyObj.setOutFields(keyFields);
			context.write(mapKeyObj, one);
		}
		
		/**
		 * 从 UID 中解析得到玩家的新增时间，66 UID 信息如下：
		 * QZF665CBE07F24C30C8F57252AE804C3F3153E0F42AB620A8EB0EE931BB131D4F8
		 * 前两位 QZ|QQ|WX 是应用平台，可以直接截取掉
		 * 后面64位使用Tea加密，秘钥是 : tencent#X@~g()[]
		 * 解开后是16byte的数组，由两个long型组成
		 * 第一个long是生成时间戳，到毫秒, 第二个long是IP地址
		 */
		
		if(66 != uid.length()){
			//旧版 uid 不是 66 位，扔掉
			return;
		}
		
		int newAddDate = parseForNewAddDate(uid);
		int days = (statDate - newAddDate)/3600/24;
		if(days <=0 || days > 30){
			//只需计算 30 天留存
			return;
		}
		
		String[] keyFields = new String[] { 
				appId,
				platform,
				H5_PromotionAPP,
				H5_DOMAIN,
				H5_REF,
				Constants.PLAYER_TYPE_NEWADD,
				days + ""
		};
		
		mapKeyObj.setSuffix(Constants.SUFFIX_H5_30DAY_RETAIN_SUM);
		mapKeyObj.setOutFields(keyFields);
		context.write(mapKeyObj, one);
	}
	
	private int parseForNewAddDate(String uid){
		//原始 UID 中前面两位是推广平台，后面 64 位是真实 UID
		String uid2 = uid.substring(2);
		byte[] b = new TEACoding("tencent#X@~g()[]".getBytes()).decodeFromHexStr(uid2);
		long l = Bytes.toLong(b, 0, 8);
		cal.setTimeInMillis(l);
		
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return (int) (cal.getTimeInMillis() / 1000);
	}
}
