package test.html5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.html5.H5PageViewForAppMapper;
import net.digitcube.hadoop.mapreduce.html5.H5PageViewForAppReducer;
import net.digitcube.hadoop.mapreduce.html5.H5PageViewForPlayerMapper;
import net.digitcube.hadoop.mapreduce.html5.H5PageViewForPlayerReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class H5PageViewForPlayerTester {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver2;
	
	@Before
	public void setUp() {
		H5PageViewForPlayerMapper map = new H5PageViewForPlayerMapper();
		H5PageViewForPlayerReducer reduce = new H5PageViewForPlayerReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		H5PageViewForAppMapper m = new H5PageViewForAppMapper();
		H5PageViewForAppReducer r = new H5PageViewForAppReducer();
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(m, r);
	}

	@Test
	public void testNewActDevicePlayer() throws Exception {
		
		LongWritable longWritable = new LongWritable();
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\part-0927-DC_PV"));
		while(null != (line = br.readLine())){
			mapReduceDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList){
			String s = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(s);
			mapReduceDriver2.withInput(longWritable, new Text(s));
		}
		
		System.out.println("=========================================");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList2 = mapReduceDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : resultList2){
			String s = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(s);
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

