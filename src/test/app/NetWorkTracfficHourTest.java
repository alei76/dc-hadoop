package test.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.app.AppViewAndDownloadMapper;
import net.digitcube.hadoop.mapreduce.app.AppViewAndDownloadReducer;
import net.digitcube.hadoop.mapreduce.app.KeyWordSearchStatMapper;
import net.digitcube.hadoop.mapreduce.app.KeyWordSearchStatReducer;
import net.digitcube.hadoop.mapreduce.app.NetworkTrafficHourMapper;
import net.digitcube.hadoop.mapreduce.app.NetworkTrafficHourReducer;
import net.digitcube.hadoop.mapreduce.app.PageAndModuleRollingMapper;
import net.digitcube.hadoop.mapreduce.app.PageAndModuleRollingReducer;
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

public class NetWorkTracfficHourTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, LongWritable> eventDriver;
	
	@Before
	public void setUp() {
		NetworkTrafficHourMapper map = new NetworkTrafficHourMapper();
		NetworkTrafficHourReducer reduce = new NetworkTrafficHourReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\part-r-00000-DESelf_APP_TRAFFIC"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, LongWritable>> onlineList = eventDriver.run();
		
		for(Pair<OutFieldsBaseModel, LongWritable> pair : onlineList){
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(lin);
		}
		br.close();
	}
}

