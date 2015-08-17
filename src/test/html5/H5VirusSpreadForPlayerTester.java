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
import net.digitcube.hadoop.mapreduce.html5.H5PageViewForAppMapper;
import net.digitcube.hadoop.mapreduce.html5.H5PageViewForAppReducer;
import net.digitcube.hadoop.mapreduce.html5.H5PageViewForPlayerMapper;
import net.digitcube.hadoop.mapreduce.html5.H5PageViewForPlayerReducer;
import net.digitcube.hadoop.mapreduce.html5.H5VirusSpreadForAppMapper;
import net.digitcube.hadoop.mapreduce.html5.H5VirusSpreadForAppReducer;
import net.digitcube.hadoop.mapreduce.html5.H5VirusSpreadForPlayerMapper;
import net.digitcube.hadoop.mapreduce.html5.H5VirusSpreadForPlayerReducer;
import net.digitcube.hadoop.mapreduce.html5.H5VirusSpreadTop100Mapper;
import net.digitcube.hadoop.mapreduce.html5.H5VirusSpreadTop100Reducer;
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

public class H5VirusSpreadForPlayerTester {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> playerDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> pvDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver2;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver3;
	
	@Before
	public void setUp() {
		H5OnlineDayForPlayerMapper m1 = new H5OnlineDayForPlayerMapper();
		H5OnlineDayForPlayerReducer r1 = new H5OnlineDayForPlayerReducer();
		playerDriver = MapReduceDriver.newMapReduceDriver(m1, r1);
		
		H5PageViewForPlayerMapper m2 = new H5PageViewForPlayerMapper();
		H5PageViewForPlayerReducer r2 = new H5PageViewForPlayerReducer();
		pvDriver = MapReduceDriver.newMapReduceDriver(m2, r2);
		
		H5VirusSpreadForPlayerMapper m = new H5VirusSpreadForPlayerMapper();
		H5VirusSpreadForPlayerReducer r = new H5VirusSpreadForPlayerReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(m, r);
		
		H5VirusSpreadForAppMapper mm = new H5VirusSpreadForAppMapper();
		H5VirusSpreadForAppReducer rr = new H5VirusSpreadForAppReducer();
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(mm, rr);
		
		H5VirusSpreadTop100Mapper mmm = new H5VirusSpreadTop100Mapper();
		H5VirusSpreadTop100Reducer rrr = new H5VirusSpreadTop100Reducer();
		mapReduceDriver3 = MapReduceDriver.newMapReduceDriver(mmm, rrr);
	}

	@Test
	public void testNewActDevicePlayer() throws Exception {
		
		LongWritable longWritable = new LongWritable();
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\Online.0927"));
		while(null != (line = br.readLine())){
			playerDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list1 = playerDriver.run();
		
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\part-0927-DC_PV"));
		while(null != (line = br.readLine())){
			pvDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list2 = pvDriver.run();
		
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\UserInfo.0927"));
		while(null != (line = br.readLine())){
			mapReduceDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		
		//playerDriver
		System.out.println("Player------------");
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> list : list1){
			line = list.getFirst().toString() + "\t" + list.getSecond().toString();
			mapReduceDriver.withInput(longWritable, new Text(line));
			//System.out.println(line);
		}
		
		//pvDriver
		System.out.println("pvDriver------------");
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> list : list2){
			line = list.getFirst().toString() + "\t" + list.getSecond().toString();
			mapReduceDriver.withInput(longWritable, new Text(line));
			//System.out.println(line);
		}
		System.out.println("Virus------------");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list3 = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> list : list3){
			line = list.getFirst().toString() + "\t" + list.getSecond().toString();
			mapReduceDriver2.withInput(longWritable, new Text(line));
			System.out.println(line);
		}
		
		System.out.println("Virus2------------");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list4 = mapReduceDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> list : list4){
			line = list.getFirst().toString() + "\t" + list.getSecond().toString();
			mapReduceDriver3.withInput(longWritable, new Text(line));
			System.out.println(line);
		}
		
		System.out.println("Virus3------------");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list5 = mapReduceDriver3.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> list : list5){
			line = list.getFirst().toString() + "\t" + list.getSecond().toString();
			System.out.println(line);
		}
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

