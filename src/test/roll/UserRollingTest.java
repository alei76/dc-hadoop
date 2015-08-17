package test.roll;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.TaskStatMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.TaskStatReducer;
import net.digitcube.hadoop.mapreduce.userroll.UserExtInfoRollingDayMap;
import net.digitcube.hadoop.mapreduce.userroll.UserExtInfoRollingDayReducer;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingDayMapper;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingDayReducer;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingHourMapper;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingHourReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class UserRollingTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> eventDriver;
	
	@Before
	public void setUp() {
		UserInfoRollingDayMapper map = new UserInfoRollingDayMapper();
		UserInfoRollingDayReducer reduce = new UserInfoRollingDayReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		LongWritable longWritable = new LongWritable();
		
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\info_day.0210"));
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\online_day.0210"));
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\pay_day.0210"));
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\roll.0209"));
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		
		//job1
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = eventDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString());
		}
	}
}

