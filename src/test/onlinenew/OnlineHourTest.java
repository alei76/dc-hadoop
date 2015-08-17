package test.onlinenew;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class OnlineHourTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> errorReportDriver;

	@Before
	public void setUp() {
		OnlineHourMapper map = new OnlineHourMapper();
		OnlineHourReducer reduce = new OnlineHourReducer();
		errorReportDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testErrorReport() throws Exception {
		LongWritable longWritable = new LongWritable();

		
		String line = null;
		BufferedReader reader = new BufferedReader(new FileReader(
				"C:\\Users\\Administrator\\Desktop\\aaa"));
		while (null != (line = reader.readLine())) {
			errorReportDriver.withInput(longWritable, new Text(line));
		}
		reader.close();
		List<Pair<OutFieldsBaseModel, NullWritable>> resultList = errorReportDriver.run();

		for (Pair<OutFieldsBaseModel, NullWritable> pair : resultList) {
			System.out.println(pair.getFirst().getSuffix() + " : "
					+ pair.getFirst().toString() + "\t"
					+ pair.getSecond().toString());
		}
	}
}
