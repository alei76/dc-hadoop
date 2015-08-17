package test.event;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceMapper;
import net.digitcube.hadoop.mapreduce.accountmonitor.AccountNumPerDeviceReducer;
import net.digitcube.hadoop.mapreduce.event.EventStatisticsMapper;
import net.digitcube.hadoop.mapreduce.event.EventStatisticsReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import test.onlinenew.TestDataUtil;

public class EventStatasticsTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	
	@Before
	public void setUp() {
		EventStatisticsMapper map = new EventStatisticsMapper();
		EventStatisticsReducer reduce = new EventStatisticsReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		/*BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\event.0915"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}*/
		String s = "1410779113		937042C1192B1833CD8DF4895B281674|v0.0.1	A5410353F49B4005E076BFC0ECBDF4A6	A5410353F49B4005E076BFC0ECBDF4A6	3	weixin	0	0	0	-	1440*900	unknown	unknown	3	中国	广东省	中国电信	-	-	-	183.16.198.196	-	0	sell_goods	1	{\"data\":\"some data\"}";
		LongWritable longWritable = new LongWritable();
		eventDriver.withInput(longWritable, new Text(s));
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = eventDriver.run();
		
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			System.out.println("---" + pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
	}
}

