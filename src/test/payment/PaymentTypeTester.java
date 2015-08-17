package test.payment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.payment.PaymentTypeDayMapper;
import net.digitcube.hadoop.mapreduce.payment.PaymentTypeDayReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PaymentTypeTester {

	// private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver;
	private MapDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> mapDriver;

	@Before
	public void setUp() {
		PaymentTypeDayMapper map = new PaymentTypeDayMapper();
		PaymentTypeDayReducer reduce = new PaymentTypeDayReducer();
		mapDriver = MapDriver.newMapDriver(map);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

//	@Test
	public void testMap() throws Exception {
		System.out.println("MapReduce,Begin------------------------------------------------------------------");
		// 多个输入源
		BufferedReader[] arr = { new BufferedReader(new FileReader("D:\\hadoop\\payment.0511")) };
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list = mapDriver.run();
		Collections.sort(list, new Comparator<Pair<OutFieldsBaseModel, OutFieldsBaseModel>>() {
			@Override
			public int compare(Pair<OutFieldsBaseModel, OutFieldsBaseModel> o1,
					Pair<OutFieldsBaseModel, OutFieldsBaseModel> o2) {
				if (o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix()) > 0) {
					return 1;
				} else if (o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix()) < 0) {
					return -1;
				}
				return 0;
			}
		});
		for (Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : list) {
			String lin = pair.getFirst().getSuffix() + "\t" + pair.getFirst().toString() + "\t"
					+ pair.getSecond().toString();
			System.out.println(lin);
		}
		System.out.println("MapReduce,End------------------------------------------------------------------");
	}

	 @Test
	public void testMapReduce() throws Exception {
		System.out.println("MapReduce,Begin------------------------------------------------------------------");
		// 多个输入源
		BufferedReader[] arr = { new BufferedReader(new FileReader("D:\\hadoop\\payment.0511")) };
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> list = mapReduceDriver.run();
		Collections.sort(list, new Comparator<Pair<OutFieldsBaseModel, NullWritable>>() {
			@Override
			public int compare(Pair<OutFieldsBaseModel, NullWritable> o1, Pair<OutFieldsBaseModel, NullWritable> o2) {
				if (o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix()) > 0) {
					return 1;
				} else if (o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix()) < 0) {
					return -1;
				}
				return 0;
			}
		});
		for (Pair<OutFieldsBaseModel, NullWritable> pair : list) {
			String lin = pair.getFirst().getSuffix() + "\t" + pair.getFirst().toString();
			System.out.println(lin);
		}
		System.out.println("MapReduce,End------------------------------------------------------------------");
	}
}
