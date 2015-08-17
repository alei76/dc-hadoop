package test.warehouse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.WHPaymentDetailMapper;
import net.digitcube.hadoop.mapreduce.warehouse.WHPaymentDetailReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class WHPaymentDetailTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WHPaymentDetailMapper map = new WHPaymentDetailMapper();
		WHPaymentDetailReducer reduce = new WHPaymentDetailReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);

	}

	@Test
	public void testMapReduce() throws Exception {
		System.out.println("MapReduce,Begin------------------------------------------------------------------");
		// 多个输入源
		BufferedReader[] arr = { new BufferedReader(new FileReader("D:\\hdfs\\warehouse\\payment.log")) };
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = mapReduceDriver.run();
		Collections.sort(onlineList, new Comparator<Pair<OutFieldsBaseModel, NullWritable>>() {
			@Override
			public int compare(Pair<OutFieldsBaseModel, NullWritable> o1, Pair<OutFieldsBaseModel, NullWritable> o2) {
				if (o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix()) > 0) {
					return -1;
				}
				return 0;
			}
		});
		for (Pair<OutFieldsBaseModel, NullWritable> pair : onlineList) {
			String lin = pair.getFirst().toString();
			System.out.println(lin);
		}
		System.out.println("MapReduce,End------------------------------------------------------------------");
	}

}
