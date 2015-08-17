package test.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.TmpOutFieldsBaseModel;
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
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PageAndModuleTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	
	@Before
	public void setUp() {
		PageAndModuleRollingMapper map = new PageAndModuleRollingMapper();
		PageAndModuleRollingReducer reduce = new PageAndModuleRollingReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\module"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			EventLog event = new EventLog(line.split("\t"));
			int duration = event.getDuration();
			String moduleName = event.getArrtMap().get("moduleName");
			String resumeTime = event.getArrtMap().get("resumeTime");
			
			String appId = event.getAppID();
			String platform = event.getPlatform();
			String channel = event.getChannel();
			String gameServer = event.getGameServer();
			String accountId = event.getAccountID();
			
			if(null == moduleName ||
					null == resumeTime ||
					null == appId ||
					null == platform ||
					null == channel ||
					null == gameServer ||
					null == accountId
					){
				System.out.println(line);
			}
		}
		br.close();
	}
}

