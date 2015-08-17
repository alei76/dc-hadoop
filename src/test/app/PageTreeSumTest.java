package test.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.app.PageTreeForLoginTimeMapper;
import net.digitcube.hadoop.mapreduce.app.PageTreeForLoginTimeReducer;
import net.digitcube.hadoop.mapreduce.app.PageTreeSumMapper;
import net.digitcube.hadoop.mapreduce.app.PageTreeSumReducer;
import net.digitcube.hadoop.mapreduce.roomtimes.RoomPlayTimesForCherryMapper;
import net.digitcube.hadoop.mapreduce.roomtimes.RoomPlayTimesForCherryReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PageTreeSumTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, Text> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver2;
	
	@Before
	public void setUp() {
		PageTreeForLoginTimeMapper map = new PageTreeForLoginTimeMapper();
		PageTreeForLoginTimeReducer reduce = new PageTreeForLoginTimeReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		PageTreeSumMapper m = new PageTreeSumMapper();;
		PageTreeSumReducer r = new PageTreeSumReducer();
		eventDriver2 = MapReduceDriver.newMapReduceDriver(m, r);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\treeForPlayer.txt"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, Text>> onlineList = eventDriver.run();
		
		for(Pair<OutFieldsBaseModel, Text> pair : onlineList){
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println("---"+lin);
			
			eventDriver2.withInput(longWritable, new Text(lin));
		}
		
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList = eventDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList){
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(lin);
		}
		br.close();
	}
}

