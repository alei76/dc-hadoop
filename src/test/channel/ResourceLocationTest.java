package test.channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHResourceLocationMapper;
import net.digitcube.hadoop.mapreduce.channel.CHResourceLocationReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class ResourceLocationTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> eventDriver;

	@Before
	public void setUp() {
		CHResourceLocationMapper map = new CHResourceLocationMapper();
		CHResourceLocationReducer reduce = new CHResourceLocationReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void test() throws Exception {
		// 多个输入源
		BufferedReader[] arr = {
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_Channel_RL_Click")),
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_Channel_RL_Show")), };
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				eventDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, IntWritable>> onlineList = eventDriver.run();

		for (Pair<OutFieldsBaseModel, IntWritable> pair : onlineList) {
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString() + "\t"
					+ pair.getFirst().getSuffix();
			System.out.println(lin);
		}
	}
}
