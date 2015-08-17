package test.warehouse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.warehouse.TmpWHMapper;
import net.digitcube.hadoop.mapreduce.warehouse.TmpWHReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TmpWHTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;
	private MapDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> mapDriver;

	@Before
	public void setUp() {
		TmpWHMapper map = new TmpWHMapper();
		TmpWHReducer reduce = new TmpWHReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		mapDriver = MapDriver.newMapDriver(map);
	}

	@Test
	public void testMapReduce() throws Exception {
		System.out.println("MapReduce,Begin------------------------------------------------------------------");
		// 多个输入源
		BufferedReader[] arr = { new BufferedReader(new FileReader("D:\\hdfs\\warehouse\\wh_device_rolling.log")) };
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list = mapReduceDriver.run();
		Collections.sort(list, new Comparator<Pair<OutFieldsBaseModel, OutFieldsBaseModel>>() {
			@Override
			public int compare(Pair<OutFieldsBaseModel, OutFieldsBaseModel> o1, Pair<OutFieldsBaseModel, OutFieldsBaseModel> o2) {
				if (o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix()) > 0) {
					return -1;
				}
				return 0;
			}
		});
		for (Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : list) {
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString() + "\t" + pair.getFirst().getSuffix();
			System.out.println(lin);
		}
		System.out.println("MapReduce,End------------------------------------------------------------------");
	}
	
//	@Test
	public void test(){
		byte ONE = 1;
		byte TWO = 2;
		byte FOUR = 4;
		byte ENGHT = 8;
		System.out.println(ONE | TWO);
	}
	
}
