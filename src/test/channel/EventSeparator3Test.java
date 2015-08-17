package test.channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.event.CHEventSeparatorMapper;
import net.digitcube.hadoop.mapreduce.channel.event.CHEventSeparatorReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class EventSeparator3Test {

	private MapReduceDriver<LongWritable, Text, Text, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> eventDriver;

	@Before
	public void setUp() {
		CHEventSeparatorMapper map = new CHEventSeparatorMapper();
		CHEventSeparatorReducer reduce = new CHEventSeparatorReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void test() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("D:\\hadoop\\EventSelf-2015-03-05-17.log"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while (null != (line = br.readLine())) {
			eventDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = eventDriver.run();

		for (Pair<OutFieldsBaseModel, NullWritable> pair : onlineList) {
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString() + "\t"
					+ pair.getFirst().getSuffix();
			System.out.println(lin);
		}
		br.close();
	}
}
