package test.gameexperience;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.gameexperience.GameExperienceDayMapper;
import net.digitcube.hadoop.mapreduce.gameexperience.GameExperienceDayReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GameExperienceDayTest {
	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> gameExperienceDayDriver;

	@Before
	public void setUp() {
		GameExperienceDayMapper map = new GameExperienceDayMapper();
		GameExperienceDayReducer reduce = new GameExperienceDayReducer();
		gameExperienceDayDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}
	
	@Test
	public void testGameExperienceDay() throws Exception {
		LongWritable longWritable = new LongWritable();
		String line = null;
		BufferedReader reader = new BufferedReader(new FileReader(
				"F:\\event.log"));
		while (null != (line = reader.readLine())) {
			gameExperienceDayDriver.withInput(longWritable, new Text(line));
		}
		reader.close();
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList = gameExperienceDayDriver.run();

		for (Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList) {
			System.out.println(pair.getFirst().getSuffix() + " : "
					+ pair.getFirst().toString() + "\t"
					+ pair.getSecond().toString());
		}
		
		
		
		
		
	}
}

