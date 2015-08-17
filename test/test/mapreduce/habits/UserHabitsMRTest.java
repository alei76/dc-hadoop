package test.mapreduce.habits;

import java.util.ArrayList;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.habits.UserHabitsMapper;
import net.digitcube.hadoop.mapreduce.habits.UserHabitsReducer;

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

public class UserHabitsMRTest {
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;

	@Before
	public void setUp() {
		UserHabitsMapper map = new UserHabitsMapper();
		UserHabitsReducer red = new UserHabitsReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, red);
	}

	@Test
	public void testIdentityMapper() throws Exception {
		List<Text> inputList = new ArrayList<Text>();
		String inputString = "appid\tIOS\tchannelId\tgameServer\t3\t2\t1105\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t2\t2\t1105\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t3\t2\t593\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t2\t2\t593\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t3\t2\t732\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t2\t2\t732\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t3\t2\t1359\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t2\t2\t1359\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t3\t2\t625\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t2\t2\t625\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t3\t2\t455\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t2\t2\t455\t3\t5\n"
				+ "appid\tIOS\tchannelId\tgameServer\t2\t2\t455\t3\t5\n";
		String[] lines = inputString.split("\n");
		for (String s : lines)
			inputList.add(new Text(s));

		for (Text in : inputList) {
			mapReduceDriver.withInput(new LongWritable(), in);
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> result = mapReduceDriver
				.run();

		for (Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : result) {
			System.out.println(pair.getFirst().toString() + " : "
					+ pair.getSecond().toString());
		}
	}
}
