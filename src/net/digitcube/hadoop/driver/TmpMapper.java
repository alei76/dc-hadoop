package net.digitcube.hadoop.driver;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.util.IOSChannelUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 主要逻辑：
 * 对不同的自定义事件根据 ID 进行分离
 * 并且以自定义事件的 ID 作为输出后缀 
 *
 */
public class TmpMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Text keyObj = new Text();

	private Set<String> set = new HashSet<String>();
	Calendar cal = Calendar.getInstance();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		set.add("0774803F288363D0E9612024CD170730");
		set.add("08F62B2D0162A214F834E9C70164D88E");
		set.add("0C3CA43F3F49431603A4292B512B5F2E");
		set.add("173D272EBFEC07ABD9ABAA6F1B6D66E8");
		set.add("19B5032BE9B55B90FBBE56C88CA273A9");
		set.add("1CFD65FD7ABC0FCE95D998C8E8C217D5");
		set.add("1EC62A49F922EE50B1C67B9D93C3E0AC");
		set.add("27BA6D67B5323D308227E617B58E09DA");
		set.add("2FF1E55D0920518B49F05F8BBA5F5739");
		set.add("31BEC46980ED9BC92CE197284A45C961");
		set.add("348F3731F008211EBCAE203C9E5150C1");
		set.add("3614FC478F6DBF37C584D09BD5A55B7C");
		set.add("509AA6D8E8FA4324CB1E3C10B08D9CFB");
		set.add("56DA116618D95B50AF1A874B10ED210B");
		set.add("5FAC0340EC0B0DDAD8603C15B4BB3941");
		set.add("62AB37BBAF01F9D0819654ABA0D3227C");
		set.add("6DE829703A0F01BFEFCDFF0F73772869");
		set.add("777B35527878720F934154BEDCE33792");
		set.add("7A376D7F33A5BD19836C1C3AE49CC223");
		set.add("8B2F607E91FEE002CF9D20BA7118DD34");
		set.add("8CA573138B6B9A6E95205D1C7220659A");
		set.add("8E14EC0375A8A3A25B6ED1A6D8F0627C");
		set.add("8ED892180179B95D10B4A493C3E260F3");
		set.add("AD0BE7A3619115AE51B03381BBE4C0AE");
		set.add("AF22A9D734A4A6D89FE86589ECC45213");
		set.add("B7246C26E42DAD87863A895171579A51");
		set.add("B95BE7754CFDA1509DD1A169CC30B531");
		set.add("B9EB060C756BEA3A4AC37DF2E6A8B7B9");
		set.add("BB27BB46C9F904D8A74568F2253DA143");
		set.add("BB8730E2E1A1E1B9FC4DB2620E976F8E");
		set.add("BFCC8709F97D8C3861D8D67CA0D34C8B");
		set.add("C2F0CD08D971EE71D1A27072BE1A1716");
		set.add("DB7E3BC9239313419EDC16A95B20DE48");
		set.add("E89024F318190E24F14A6B02CE45F4C0");
		set.add("F2E3C4DE7EC3E8E300B023BA9F1EF118");
		set.add("FEE03C4E5DFF96EC2603FE3B77AD9D6E");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		UserInfoRollingLog log = new UserInfoRollingLog(cal.getTime(), paraArr);
		if(log.getAppID().contains("5483E69E7F476719CBDBB2D62171AF81")){
			String accId = log.getAccountID();
			if(set.contains(accId) && log.getPlayerDayInfo().getGameRegion().equals(MRConstants.ALL_GAMESERVER)){
				cal.setTimeInMillis(1000L * log.getPlayerDayInfo().getFirstLoginDate());
				String newAddDate = sdf.format(cal.getTime());
				keyObj.set(accId + "\t" + newAddDate + "\t" + log.getAppID());
				context.write(keyObj, NullWritable.get());
			}
		}
		
	}
}
