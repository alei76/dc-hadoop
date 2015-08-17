package test.onlinenew;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.OnlineDayLog;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineDayMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineDayReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class OnlineDayTester {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver2;
	
	@Before
	public void setUp() {
		OnlineDayMapper map = new OnlineDayMapper();
		OnlineDayReducer reduce = new OnlineDayReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		OnlineHourMapper m = new OnlineHourMapper();
		OnlineHourReducer p = new OnlineHourReducer();
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(m, p);
	}

	@Test
	public void testPalyerOnlineHour() throws Exception {
		LongWritable longWritable = new LongWritable();
		String s3 = "1405958400	4	CE9812F8FDA0920535FF38DE5171AB80|2.1	2a27c46578b0531027394c3a9777a7f8dfac6c26	tongbu_4178641	1	platformCommon	0	0	0	g8-13服-机械战警	1536*2048	5.1.1	iPad3	2	澳大利亚	unknown	TPG Internet Pty Ltd	-	3627339	-	14.201.242.250	-	1405951698	6704	37	";
		String s4 = "1405958400	4	CE9812F8FDA0920535FF38DE5171AB80|2.0	2a27c46578b0531027394c3a9777a7f8dfac6c26	tongbu_4178641	1	platformCommon	0	0	0	g8-13服-机械战警	1536*2048	5.1.1	iPad3	2	澳大利亚	unknown	TPG Internet Pty Ltd	-	3627339	-	14.201.242.250	-	1405951699	6705	38	";
		mapReduceDriver2.withInput(longWritable, new Text(s3));
		mapReduceDriver2.withInput(longWritable, new Text(s4));
		List<Pair<OutFieldsBaseModel, NullWritable>>  result = mapReduceDriver2.run();
		for(Pair<OutFieldsBaseModel, NullWritable> r : result){
			//System.out.println(r.getFirst().toString());
			mapReduceDriver.withInput(longWritable, new Text(r.getFirst().toString()));
		}
		
		result = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> r : result){
			if(!r.getFirst().toString().contains("ALL_GS")){
				System.out.println(r.getFirst().toString());
			}
		}
	}
}

