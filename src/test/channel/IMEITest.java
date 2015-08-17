package test.channel;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoMapper;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoReducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class IMEITest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> userInfoDayDriver;
	
	@Before
	public void setUp() {
		CHUserInfoMapper map2 = new CHUserInfoMapper();
		CHUserInfoReducer red2 = new CHUserInfoReducer();
		userInfoDayDriver = MapReduceDriver.newMapReduceDriver(map2, red2);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		LongWritable longWritable = new LongWritable();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\ch_info.txt")));
		String line = null;
		while(null != (line=br.readLine())){
			userInfoDayDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		
		List<Pair<OutFieldsBaseModel, NullWritable>> upgradeList = userInfoDayDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : upgradeList){
			String result = pair.getFirst().toString();
			System.out.println(result);
		}
	}
}

