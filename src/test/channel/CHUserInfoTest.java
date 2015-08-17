package test.channel;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoMapper;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoReducer;
import net.digitcube.hadoop.tmp.datamining.IMEIOnlineMapper;
import net.digitcube.hadoop.tmp.datamining.IMEIOnlineReducer;

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class CHUserInfoTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, NullWritable, OutFieldsBaseModel, NullWritable> mapreduceDriver;
	
	@Before
	public void setUp() {
		IMEIOnlineMapper map2 = new IMEIOnlineMapper();
		IMEIOnlineReducer red2 = new IMEIOnlineReducer();
		mapreduceDriver = MapReduceDriver.newMapReduceDriver(map2, red2);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		LongWritable longWritable = new LongWritable();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\hadoop\\Online.log")));
		String line = null;
		while(null != (line=br.readLine())){
			mapreduceDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		
		List<Pair<OutFieldsBaseModel, NullWritable>> upgradeList = mapreduceDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : upgradeList){
			String result = pair.getFirst().toString();
			System.out.println(result);
		}
	}
}

