package test.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.app.PageTreeForLoginTimeMapper;
import net.digitcube.hadoop.mapreduce.app.PageTreeForLoginTimeReducer;
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

public class PageTreeForLoginTimeTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, Text> eventDriver;
	
	@Before
	public void setUp() {
		PageTreeForLoginTimeMapper map = new PageTreeForLoginTimeMapper();
		PageTreeForLoginTimeReducer reduce = new PageTreeForLoginTimeReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
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
			System.out.println(pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
		br.close();
	}
}

