package test.channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHPageTreeForLoginTimeMapper;
import net.digitcube.hadoop.mapreduce.channel.CHPageTreeForLoginTimeReducer;
import net.digitcube.hadoop.mapreduce.channel.CHPageTreeSumMapper;
import net.digitcube.hadoop.mapreduce.channel.CHPageTreeSumReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class CHPageTreeSumTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, Text> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriverSum;

	@Before
	public void setUp() {
		CHPageTreeForLoginTimeMapper map = new CHPageTreeForLoginTimeMapper();
		CHPageTreeForLoginTimeReducer reduce = new CHPageTreeForLoginTimeReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		CHPageTreeSumMapper mapSum = new CHPageTreeSumMapper();
		CHPageTreeSumReducer reduceSum = new CHPageTreeSumReducer();
		mapReduceDriverSum = MapReduceDriver.newMapReduceDriver(mapSum, reduceSum);
	}

//	@Test
	public void testMapReduce() throws Exception {
		System.out.println("MapReduce,Begin------------------------------------------------------------------");
		// 多个输入源
		BufferedReader[] arr = { new BufferedReader(new FileReader("D:\\hadoop\\DESelf_Channel_PageNavigation.log")) };
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, Text>> onlineList = mapReduceDriver.run();
		Collections.sort(onlineList, new Comparator<Pair<OutFieldsBaseModel, Text>>() {
			@Override
			public int compare(Pair<OutFieldsBaseModel, Text> o1,
					Pair<OutFieldsBaseModel, Text> o2) {
				if (o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix()) > 0) {
					return -1;
				}
				return 0;
			}
		});
		for (Pair<OutFieldsBaseModel, Text> pair : onlineList) {
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString() + "\t";
			System.out.println(lin);
		}
		System.out.println("MapReduce,End------------------------------------------------------------------");
		// 读取前一个reducer的结果
	}
	
	@Test
	public void testMapReduceSum() throws Exception {
		System.out.println("MapReduce,Begin------------------------------------------------------------------");
		// 多个输入源
		BufferedReader[] arr = { new BufferedReader(new FileReader("D:\\hadoop\\CHANNEL_PAGETREE_4_LOGINTIME.log")) };
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriverSum.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = mapReduceDriverSum.run();
		Collections.sort(onlineList, new Comparator<Pair<OutFieldsBaseModel, OutFieldsBaseModel>>() {
			@Override
			public int compare(Pair<OutFieldsBaseModel, OutFieldsBaseModel> o1,
					Pair<OutFieldsBaseModel, OutFieldsBaseModel> o2) {
				if (o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix()) > 0) {
					return -1;
				}
				return 0;
			}
		});
		for (Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList) {
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString() + "\t";
			System.out.println(lin);
		}
		System.out.println("MapReduce,End------------------------------------------------------------------");
		// 读取前一个reducer的结果
	}
}
