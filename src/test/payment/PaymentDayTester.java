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
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.OnlineHourReducer;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayMapper;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayReducer;
import net.digitcube.hadoop.mapreduce.payment.PaymentStatDayMapper;
import net.digitcube.hadoop.mapreduce.payment.PaymentStatDayReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PaymentDayTester {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		PaymentStatDayMapper map = new PaymentStatDayMapper();
		PaymentStatDayReducer reduce = new PaymentStatDayReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testPalyerOnlineHour() throws Exception {
		Set<String> set = new HashSet<String>();
		int count = 0;
		String line = null;
		LongWritable longWritable = new LongWritable();
		BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\error_pay"));
		while(null != (line = reader.readLine())){
			/*String[] array = line.split(MRConstants.SEPERATOR_IN);
			PaymentDayLog paymentDayLog = new PaymentDayLog(array);
			set.add(paymentDayLog.getExtend().getResolution());
			if("1".equals(paymentDayLog.getExtend().getResolution())){
				count++;
			}*/
			mapReduceDriver.withInput(longWritable, new Text(line));
		}
		reader.close();
		
		List<Pair<OutFieldsBaseModel, NullWritable>>  list = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : list){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString());
		}
	}
}

