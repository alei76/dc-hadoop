package test.payment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayMapper;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayReducer;
import net.digitcube.hadoop.mapreduce.payment.WhalePlayerMapper;
import net.digitcube.hadoop.mapreduce.payment.WhalePlayerReducer;
import net.digitcube.hadoop.mapreduce.payment.WhalePlayerSumMapper;
import net.digitcube.hadoop.mapreduce.payment.WhalePlayerSumReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class WhalePlayerTester {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver2;
	
	@Before
	public void setUp() {
		WhalePlayerMapper map = new WhalePlayerMapper();
		WhalePlayerReducer reduce = new WhalePlayerReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		WhalePlayerSumMapper m = new WhalePlayerSumMapper();
		WhalePlayerSumReducer r = new WhalePlayerSumReducer();
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(m, r);
	}

	@Test
	public void testPalyerOnlineHour() throws Exception {
		LongWritable longWritable = new LongWritable();
		
		String line = null;
		BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\ruide.whale.0721"));
		while(null != (line = reader.readLine())){
			mapReduceDriver.withInput(longWritable, new Text(line.split(":")[1]));
		}
		reader.close();
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = mapReduceDriver.run();
		
		int total = 0;
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			String l = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			//System.out.println(l);
			mapReduceDriver2.withInput(longWritable, new Text(l));
			total += Integer.valueOf(pair.getSecond().getOutFields()[2]);
		}
		
		System.out.println("-----------------------------------" + total);
		
		total = 0;
		List<Pair<OutFieldsBaseModel, NullWritable>> result = mapReduceDriver2.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : result){
			String l = pair.getFirst().toString();
			System.out.println(l);
			total += Integer.valueOf(pair.getFirst().getOutFields()[7]);
		}
		System.out.println("-----------------------------------" + total);
	}
}

