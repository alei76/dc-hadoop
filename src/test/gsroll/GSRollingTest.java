package test.gsroll;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.ActRegSeparateMapper;
import net.digitcube.hadoop.mapreduce.gsroll.GSRollForPlayerMapper;
import net.digitcube.hadoop.mapreduce.gsroll.GSRollForPlayerReducer;
import net.digitcube.hadoop.mapreduce.uidroll.UIDRollingDayMapper;
import net.digitcube.hadoop.mapreduce.uidroll.UIDRollingDayReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import test.onlinenew.TestDataUtil;

public class GSRollingTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> upgradeDriver;
	@Before
	public void setUp() {
		GSRollForPlayerMapper map = new GSRollForPlayerMapper();
		GSRollForPlayerReducer reduce = new GSRollForPlayerReducer();
		upgradeDriver = new MapReduceDriver(map, reduce);
	}

	@Test
	public void testIdentityMapper() throws Exception {
		String line = null;
		//BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\shenhai.1111"));
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\sh_all"));
		while(null != (line = br.readLine())){
			upgradeDriver.withInput(new LongWritable(), new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, NullWritable>> resultList = upgradeDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> p : resultList){
			System.out.println(p.getFirst().getSuffix() + " : " + p.getFirst().toString());
		}
	}

}
