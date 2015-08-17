package test.errorreport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.errorreport.ErrorReportMapper;
import net.digitcube.hadoop.mapreduce.errorreport.ErrorReportReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class ErrorReportTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> errorReportDriver;

	@Before
	public void setUp() {
		ErrorReportMapper map = new ErrorReportMapper();
		ErrorReportReducer reduce = new ErrorReportReducer();
		errorReportDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testErrorReport() throws Exception {
		LongWritable longWritable = new LongWritable();

		
		String line = null;
		BufferedReader reader = new BufferedReader(new FileReader(
				"F:\\error\\error.915"));
		while (null != (line = reader.readLine())) {
			errorReportDriver.withInput(longWritable, new Text(line));
		}
		reader.close();
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList = errorReportDriver
				.run();

		for (Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList) {
			System.out.println(pair.getFirst().getSuffix() + " : "
					+ pair.getFirst().toString() + "\t"
					+ pair.getSecond().toString());
		}
	}
}
