package test.sdk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.sdk.SdkVersionUpdateHourMapper;
import net.digitcube.hadoop.mapreduce.sdk.SdkVersionUpdateHourReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class SdkVersionUpdateTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> sdkVersionUpdateDriver;
	
	@Before
	public void setUp() {
		SdkVersionUpdateHourMapper map = new SdkVersionUpdateHourMapper();
		SdkVersionUpdateHourReducer reduce = new SdkVersionUpdateHourReducer();
		sdkVersionUpdateDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}
	
	@Test
	public void testSdkVersionUpdateReport() throws Exception {
		LongWritable longWritable = new LongWritable();
		String line = null;
		BufferedReader reader = new BufferedReader(new FileReader(
				"F:\\2.log"));
		while (null != (line = reader.readLine())) {
			sdkVersionUpdateDriver.withInput(longWritable, new Text(line));
		}
		reader.close();
		List<Pair<OutFieldsBaseModel, NullWritable>> resultList = sdkVersionUpdateDriver.run();

		for (Pair<OutFieldsBaseModel, NullWritable> pair : resultList) {
			System.out.println(pair.getFirst().getSuffix() + " : "
					+ pair.getFirst().toString() + "\t"
					+ pair.getSecond().toString());
		}
		
		
		
		
		
	}
	
}
