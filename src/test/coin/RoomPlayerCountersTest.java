package test.coin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.roomtimes.RoomPlayTimesForCherryMapper;
import net.digitcube.hadoop.mapreduce.roomtimes.RoomPlayTimesForCherryReducer;
import net.digitcube.hadoop.mapreduce.roomtimes.RoomTimePointNumForCherryMapper;
import net.digitcube.hadoop.mapreduce.roomtimes.RoomTimePointNumForCherryReducer;
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

public class RoomPlayerCountersTest {

	private MapReduceDriver<LongWritable, Text, TmpOutFieldsBaseModel, TmpOutFieldsBaseModel, TmpOutFieldsBaseModel, NullWritable> eventDriver;
	
	@Before
	public void setUp() {
		RoomTimePointNumForCherryMapper map = new RoomTimePointNumForCherryMapper();
		RoomTimePointNumForCherryReducer reduce = new RoomTimePointNumForCherryReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\tmp.0311"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<TmpOutFieldsBaseModel, NullWritable>> onlineList = eventDriver.run();
		
		for(Pair<TmpOutFieldsBaseModel, NullWritable> pair : onlineList){
			line = pair.getFirst().toString();
			System.out.println(line);
		}
	}
}

