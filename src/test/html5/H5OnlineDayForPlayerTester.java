package test.html5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.H5OnlineDayForAppMapper;
import net.digitcube.hadoop.mapreduce.html5.H5OnlineDayForAppReducer;
import net.digitcube.hadoop.mapreduce.html5.H5OnlineDayForPlayerMapper;
import net.digitcube.hadoop.mapreduce.html5.H5OnlineDayForPlayerReducer;
import net.digitcube.hadoop.mapreduce.newadd.ActDeviceNewPlayerMapper;
import net.digitcube.hadoop.mapreduce.newadd.ActDeviceNewPlayerReducer;
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

import test.onlinenew.TestDataUtil;

public class H5OnlineDayForPlayerTester {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver2;
	
	@Before
	public void setUp() {
		H5OnlineDayForPlayerMapper map = new H5OnlineDayForPlayerMapper();
		H5OnlineDayForPlayerReducer reduce = new H5OnlineDayForPlayerReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		H5OnlineDayForAppMapper m = new H5OnlineDayForAppMapper();
		H5OnlineDayForAppReducer r = new H5OnlineDayForAppReducer();
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(m, r);
	}

	@Test
	public void testNewActDevicePlayer() throws Exception {
		
		LongWritable longWritable = new LongWritable();
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\online.1008"));
		while(null != (line = br.readLine())){
			mapReduceDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList){
			String s = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			//System.out.println(s);
			//mapReduceDriver2.withInput(longWritable, new Text(s));
		}
		/*System.out.println("=========================================");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList2 = mapReduceDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList2){
			String s = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(s);
		}*/
	}
	
/*	public static void main(String[] args){
		Map<String, String> map = new HashMap<String, String>();
		map.put(Constants.H5_APP, Constants.H5_APP);
		map.put(Constants.H5_DOMAIN, Constants.H5_DOMAIN);
		map.put(Constants.H5_REF, Constants.H5_REF);
		map.put(Constants.H5_CRTIME, Constants.H5_CRTIME);
		System.out.println(StringUtil.getJsonStr(map));
	}*/
}

