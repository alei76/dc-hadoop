package test.html5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.H5OnlineDayForAppMapper;
import net.digitcube.hadoop.mapreduce.html5.H5OnlineDayForAppReducer;
import net.digitcube.hadoop.tmp.datamining.H5AreaDistMonthMapper;
import net.digitcube.hadoop.tmp.datamining.H5AreaDistMonthReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class H5AreaDistMonthTester {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		H5AreaDistMonthMapper map = new H5AreaDistMonthMapper();
		H5AreaDistMonthReducer reduce = new H5AreaDistMonthReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);

	}

	@Test
	public void testNewActDevicePlayer() throws Exception {

		LongWritable longWritable = new LongWritable();
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("D:\\hdfs\\h5_online.log"));
		while (null != (line = br.readLine())) {
			mapReduceDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, IntWritable>> resultList = mapReduceDriver.run();
		for (Pair<OutFieldsBaseModel, IntWritable> pair : resultList) {
			String s = pair.getFirst().toString() + "\t" + pair.getSecond().toString() + "\t"
					+ pair.getFirst().getSuffix();
			System.out.println(s);
		}
	}

	/*
	 * public static void main(String[] args){ Map<String, String> map = new HashMap<String, String>();
	 * map.put(Constants.H5_APP, Constants.H5_APP); map.put(Constants.H5_DOMAIN, Constants.H5_DOMAIN);
	 * map.put(Constants.H5_REF, Constants.H5_REF); map.put(Constants.H5_CRTIME, Constants.H5_CRTIME);
	 * System.out.println(StringUtil.getJsonStr(map)); }
	 */
}
