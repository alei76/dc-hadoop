package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.model.AdLabelDetail;
import net.digitcube.hadoop.model.AdLabelLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class AdLabelInHbaseMapper extends
		Mapper<LongWritable, Text, OutFieldsBaseModel, Text> {
	private OutFieldsBaseModel mapKeyObj = new OutFieldsBaseModel();
	public final static String gameTypeConfigPath = "/data/config/gameType";
	public static Map<String, String> gameConfigMap = new HashMap<String, String>();
	private final static Gson gson = new Gson();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// try {
		// FileSystem fs = FileSystem.get(context.getConfiguration());
		// Path inFile = new Path(gameTypeConfigPath);
		// FSDataInputStream fin = fs.open(inFile);
		// BufferedReader input = new BufferedReader(new InputStreamReader(
		// fin, "UTF-8"));
		// String gameConfigStr = input.readLine();
		// String[] gameConfigArr = gameConfigStr.split(",");
		// for (String gameConfig : gameConfigArr) {
		// String[] gameAndConf = gameConfig.split(":");
		// gameConfigMap.put(gameAndConf[0], gameAndConf[1]);
		// }
		// } catch (Throwable t) {
		// t.printStackTrace();
		// }
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] paraArr = value.toString().split(MRConstants.SEPERATOR_IN);
		AdLabelLog adLabelLog = new AdLabelLog(paraArr);
		String appid = adLabelLog.getAppid().split("\\|")[0];

		mapKeyObj.setOutFields(new String[] { adLabelLog.getUid(),
				adLabelLog.getPlatform(), appid });
		// String gameType = gameConfigMap.get(adLabelLog.getAppid());
		// if (gameType == null || gameType.trim().length() == 0) {
		// return;
		// }
		AdLabelDetail adLabelDatail = new AdLabelDetail();
		adLabelDatail.setCty(adLabelLog.getCountry());
		adLabelDatail.setPro(adLabelLog.getProvince());
		if (adLabelLog.getPayAmount() > 0) {
			adLabelDatail.setPa(adLabelLog.getPayAmount());
		}
		if (adLabelLog.getPayTimes() > 0) {
			adLabelDatail.setPt(adLabelLog.getPayTimes());
		}
		context.write(mapKeyObj, new Text(gson.toJson(adLabelDatail)));
	}

}
