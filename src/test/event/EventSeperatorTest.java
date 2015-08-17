package test.event;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceMapper;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceReducer;
import net.digitcube.hadoop.mapreduce.event.EventSeparatorMapper;
import net.digitcube.hadoop.mapreduce.event.EventStatisticsMapper;
import net.digitcube.hadoop.mapreduce.event.EventStatisticsReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import test.onlinenew.TestDataUtil;

public class EventSeperatorTest {

	private MapDriver<LongWritable, Text, OutFieldsBaseModel, NullWritable> mapdriver;
	
	@Before
	public void setUp() {
		EventSeparatorMapper mapper = new EventSeparatorMapper();
		mapdriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		
		LongWritable longWritable = new LongWritable();
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\error.json"));
		String line = null;
		while(null != (line = br.readLine())){
			//System.out.println(line);
			//mapdriver.withInput(longWritable, new Text(line));
			EventLog eventLog = eventLog = new EventLog(line.split("\t"));
			System.out.println(eventLog.getArrtMap().get("content"));
		}
		br.close();
		/*List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = mapdriver.run();
		
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			System.out.println("---" + pair.getFirst().toString());
		}*/
	}
}

