package test.taskanditem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemGainLostForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemGainLostForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemGainLostSumMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemGainLostSumReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class ItemGetUseTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver2;
	
	@Before
	public void setUp() {
//		ItemGainLostForPlayerMapper map = new ItemGainLostForPlayerMapper();
//		ItemGainLostForPlayerReducer reduce = new ItemGainLostForPlayerReducer();
//		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		ItemGainLostSumMapper map2 = new ItemGainLostSumMapper();
		ItemGainLostSumReducer reduce2 = new ItemGainLostSumReducer();
		eventDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("F://aaa"));
		LongWritable longWritable = new LongWritable();
		String line = null;
//		while(null != (line = br.readLine())){
//			eventDriver.withInput(longWritable, new Text(line));
//		}
//		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = eventDriver.run();
//		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
//			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
//			if(line.contains("游戏获得")
//					&& line.contains("一见倾心")
//					&& line.contains("UC")){
//				System.out.println(line);
//			}
//		}
		
		while(null != (line = br.readLine())){
			eventDriver2.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = eventDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(line);
		}
	}
}

