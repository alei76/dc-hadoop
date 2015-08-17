package test.mapreduce.userroll;

import java.util.ArrayList;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.userroll.UserFlowMapper;
import net.digitcube.hadoop.mapreduce.userroll.UserFlowReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

/**
 * @author seonzhang email:seonzhang@digitcube.net<br>
 * @version 1.0 2013年8月3日 上午11:18:11 <br>
 * @copyrigt www.digitcube.net <br>
 */

public class UserFlowMRTest {
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		UserFlowMapper map = new UserFlowMapper();
		UserFlowReducer red = new UserFlowReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, red);
	}

	@Test
	public void testIdentityMapper() throws Exception {
		List<Text> inputList = new ArrayList<Text>();
		String inputString = "appid   IOS     channelId       gameServer      NS1\n"
				+ "appid   IOS     channelId       gameServer      PS1\n"
				+ "appid   IOS     channelId       gameServer      S1 \n"
				+ "appid   IOS     channelId       gameServer      NS1\n"
				+ "appid   IOS     channelId       gameServer      PS1\n"
				+ "appid   IOS     channelId       gameServer      S1 \n"
				+ "appid   IOS     channelId       gameServer      NS1\n"
				+ "appid   IOS     channelId       gameServer      PS1\n"
				+ "appid   IOS     channelId       gameServer      S1 \n"
				+ "appid   IOS     channelId       gameServer      NS1\n"
				+ "appid   IOS     channelId       gameServer      PS1\n"
				+ "appid   IOS     channelId       gameServer      S1 \n"
				+ "appid   IOS     channelId       gameServer      NS1\n"
				+ "appid   IOS     channelId       gameServer      PS1\n"
				+ "appid   IOS     channelId       gameServer      S1 \n"
				+ "appid   IOS     channelId       gameServer      NS1\n"
				+ "appid   IOS     channelId       gameServer      PS1\n"
				+ "appid   IOS     channelId       gameServer      S1 \n";
		String[] lines = inputString.split("\n");
		for (String s : lines)
			inputList.add(new Text(s));

		for (Text in : inputList) {
			mapReduceDriver.withInput(new LongWritable(), in);
		}
		List<Pair<OutFieldsBaseModel, IntWritable>> result = mapReduceDriver
				.run();

		for (Pair<OutFieldsBaseModel, IntWritable> pair : result) {
			System.out.println(pair.getFirst().toString() + " : "
					+ pair.getSecond().get());
		}
	}
}
