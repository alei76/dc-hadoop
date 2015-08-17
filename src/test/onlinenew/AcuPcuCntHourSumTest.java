package test.onlinenew;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntReducer;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntSumMapper;
import net.digitcube.hadoop.mapreduce.onlinenew.AcuPcuCntSumReducer;
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

public class AcuPcuCntHourSumTest {

	//private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> dependDriver1;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> dependDriver2;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> targetDriver;
	
	@Before
	public void setUp() {
		OnlineHourMapper map1 = new OnlineHourMapper();
		OnlineHourReducer reduce1 = new OnlineHourReducer();
		dependDriver1 = MapReduceDriver.newMapReduceDriver(map1, reduce1);
		
		AcuPcuCntMapper map2 = new AcuPcuCntMapper();
		AcuPcuCntReducer reduce2 = new AcuPcuCntReducer();
		dependDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
		
		AcuPcuCntSumMapper map = new AcuPcuCntSumMapper();
		AcuPcuCntSumReducer reduce = new AcuPcuCntSumReducer();
		targetDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		
		List<String> dataList = TestDataUtil.generateOnlineData(60, 10, 1);
		LongWritable longWritable = new LongWritable();
		for(String data :  dataList){
			dependDriver1.withInput(longWritable, new Text(data));
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = dependDriver1.run();
		
		
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			System.out.println("---" + pair.getFirst().toString());
			dependDriver2.withInput(longWritable, new Text(pair.getFirst().toString()));
		}
		System.out.println("\n\n\n");
		List<Pair<OutFieldsBaseModel, IntWritable>> acuPcuList = dependDriver2.run();
		
		BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream("D:/acuPcuHour.text"));
		for(Pair<OutFieldsBaseModel, IntWritable> pair : acuPcuList){
			String tmpStr = pair.getFirst().toString() + MRConstants.SEPERATOR_OUT + pair.getSecond().get();
			//System.out.println(tmpStr);
			targetDriver.withInput(longWritable, new Text(tmpStr));
			
			os.write(tmpStr.getBytes());
			os.write("\n".getBytes());
		}
		os.close();
		
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> acuPcuSumList = targetDriver.run();
		os = new BufferedOutputStream(new FileOutputStream("D:/acuPcuHourSum.text"));
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : acuPcuSumList){
			//System.out.println("===" + pair.getFirst().toString() + " : " + pair.getSecond().toString());
			String tmpStr = pair.getFirst().toString() + MRConstants.SEPERATOR_OUT + pair.getSecond().toString();
			os.write(tmpStr.getBytes());
			os.write("\n".getBytes());
		}
		os.close();
	}
}

