package test.gameexperience;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.gameexperience.SceneTreeForLoginTimeMapper;
import net.digitcube.hadoop.mapreduce.gameexperience.SceneTreeForLoginTimeReducer;
import net.digitcube.hadoop.mapreduce.gameexperience.SceneTreeSumMapper;
import net.digitcube.hadoop.mapreduce.gameexperience.SceneTreeSumReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class SceneTreeTest {
	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, Text> loginDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> sumDriver;

	@Before
	public void setUp() {
		SceneTreeForLoginTimeMapper map = new SceneTreeForLoginTimeMapper();
		SceneTreeForLoginTimeReducer reduce = new SceneTreeForLoginTimeReducer();
		loginDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		SceneTreeSumMapper map1 = new SceneTreeSumMapper();
		SceneTreeSumReducer reduce1 = new SceneTreeSumReducer();
		sumDriver = MapReduceDriver.newMapReduceDriver(map1, reduce1);
	}
	
	@Test
	public void testSceneTree() throws Exception {
		LongWritable longWritable = new LongWritable();
		String line = null;
		BufferedReader reader = new BufferedReader(new FileReader("C:/Users/Administrator/Desktop/scene4lgTm.1218"));
		while (null != (line = reader.readLine())) {
			sumDriver.withInput(longWritable, new Text(line));
		}
		reader.close();
		
		System.out.println("\n\n--------------------------------------------------------\n\n");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list = sumDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : list){
			System.out.println(pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
		
		
		
	}
	
	
}
