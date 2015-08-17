package test.layout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.layout.PayNewUnionActiveMapper;
import net.digitcube.hadoop.mapreduce.layout.PayNewUnionActiveReducer;
import net.digitcube.hadoop.mapreduce.layout.PlayerInfoLayoutMapper;
import net.digitcube.hadoop.mapreduce.layout.PlayerInfoLayoutReducer;
import net.digitcube.hadoop.mapreduce.newadd.ActDeviceNewPlayerMapper;
import net.digitcube.hadoop.mapreduce.newadd.ActDeviceNewPlayerReducer;
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

public class PlayerInfoLayoutTester {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		PlayerInfoLayoutMapper map = new PlayerInfoLayoutMapper();
		PlayerInfoLayoutReducer reduce = new PlayerInfoLayoutReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testNewActDevicePlayer() throws Exception {
		LongWritable longWritable = new LongWritable();
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\online.0403"));
		String line = null;
		while(null != (line = br.readLine())){
			mapReduceDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, IntWritable>> onlineList = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, IntWritable> pair : onlineList){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
		}
	}
}

