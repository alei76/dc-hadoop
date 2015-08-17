package net.digitcube.hadoop.mapreduce.online;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author seonzhang email:seonzhang@digitcube.net
 * @version 1.0 2013年7月15日 下午5:05:26 @copyrigt www.digitcube.net<br/>
 *          输入： <br/>
 *          key : appid,platform,accountid,logintime<br/>
 *          value:channel,accounttype,gender,age,gameserver,resolution,
 *          opersystem,brand,nettype,country,province,operators,onlinetime,level
 *          输出： <br/>
 *          appid,platform,accountid,logintime,
 *          channel,accounttype,gender,age,gameserver,resolution, opersystem
 *          ,brand,nettype,country,province,operators,MAXonlinetime,MAXlevel
 */

/**
 * use @PalyerOnlineHourMapper and @PalyerOnlineHourReducer instead
 */
@Deprecated
public class UserLoginHourReducer
		extends
		Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel reduceValueObj = new OutFieldsBaseModel();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// do some setup before map
		super.setup(context);
	}

	@Override
	protected void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context)
			throws IOException, InterruptedException {

		// 计算用户最高等级，最大在线时间
		int maxLevel = 0;
		int maxOnline = 0;
		String[] lastValue = null;
		for (OutFieldsBaseModel val : values) {
			String[] value = val.getOutFields();
			lastValue = val.getOutFields(); // 默认取最后一个
			int level = StringUtil.convertInt(value[13], 0);// level
			int online = StringUtil.convertInt(value[12], 0);// online time
			maxLevel = maxLevel > level ? maxLevel : level;
			maxOnline = maxOnline > online ? maxOnline : online;
		}
		lastValue[lastValue.length - 1] = maxLevel + "";
		lastValue[lastValue.length - 2] = maxOnline + "";
		reduceValueObj.setOutFields(lastValue);
		key.setSuffix(Constants.SUFFIX_ONLINE_HOUR);
		context.write(key, reduceValueObj);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// do some clean before map
		super.cleanup(context);
	}

}
