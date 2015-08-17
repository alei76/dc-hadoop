package test.payment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.domain.PaymentDayLog;
import net.digitcube.hadoop.mapreduce.html5.H5PaymentDayForAppMapper;
import net.digitcube.hadoop.mapreduce.html5.H5PaymentDayForAppReducer;
import net.digitcube.hadoop.mapreduce.html5.H5PaymentDayMapper;
import net.digitcube.hadoop.mapreduce.html5.H5PaymentDayReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayMapper;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class H5PaymentDayTester {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, Text, OutFieldsBaseModel, NullWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> driver2;
	
	@Before
	public void setUp() {
		H5PaymentDayMapper map = new H5PaymentDayMapper();
		H5PaymentDayReducer reduce = new H5PaymentDayReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		H5PaymentDayForAppMapper m = new H5PaymentDayForAppMapper();
		H5PaymentDayForAppReducer r = new H5PaymentDayForAppReducer();
		driver2 = MapReduceDriver.newMapReduceDriver(m, r);
	}

	@Test
	public void testPalyerOnlineHour() throws Exception {
		Set<String> set = new HashSet<String>();
		int count = 0;
		String line = null;
		LongWritable longWritable = new LongWritable();
		BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\pay.tmp"));
		while(null != (line = reader.readLine())){
			mapReduceDriver.withInput(longWritable, new Text(line));
		}
		reader.close();
		
		for(Pair<OutFieldsBaseModel, NullWritable>  pair : mapReduceDriver.run()){
			driver2.withInput(longWritable, new Text(pair.getFirst().toString()));
		}
		
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel>  pair : driver2.run()){
			System.out.println(pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
	}
}

