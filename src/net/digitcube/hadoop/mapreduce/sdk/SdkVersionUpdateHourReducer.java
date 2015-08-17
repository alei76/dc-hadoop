package net.digitcube.hadoop.mapreduce.sdk;

import java.io.IOException;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 *  key:
 *  { appID, platForm,sdkVersion };
 *  Suffix(Constants.SUFFIX_SDK_UPDATE_NOTICE);
 *  value:Null
 *  <br>
 */
public class SdkVersionUpdateHourReducer  extends Reducer<OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable>{
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {
		context.write(key, NullWritable.get());
	}

}
