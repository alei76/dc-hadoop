package test.event;

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

public class Event2SeperatorTest {

	private MapReduceDriver<LongWritable, Text, Text, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> eventDriver;
	
	@Before
	public void setUp() {
		CHEventSeparatorMapper map = new CHEventSeparatorMapper();
		CHEventSeparatorReducer reduce = new CHEventSeparatorReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		
		LongWritable longWritable = new LongWritable();
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\EventSelf-2015-03-16-19.log"));
		String line = null;
		while(null != (line = br.readLine())){
			//System.out.println(line);
			eventDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = eventDriver.run();
		
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			System.out.println("---" + pair.getFirst().toString());
		}
	}
}

