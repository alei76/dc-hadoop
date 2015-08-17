package test.roll;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.ActRegSeparateMapper;
import net.digitcube.hadoop.mapreduce.uidroll.UIDRollingDayMapper;
import net.digitcube.hadoop.mapreduce.uidroll.UIDRollingDayReducer;
import net.digitcube.hadoop.tmp.datamining.UIDIncrementMapper;
import net.digitcube.hadoop.tmp.datamining.UIDIncrementReducer;

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

public class UIDIncrementTest {

	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> upgradeDriver;
	@Before
	public void setUp() {
		UIDIncrementMapper map = new UIDIncrementMapper();
		UIDIncrementReducer reduce = new UIDIncrementReducer();
		upgradeDriver = new MapReduceDriver(map, reduce);
	}

	@Test
	public void testIdentityMapper() throws Exception {

		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\uidroll.txt"));
		while(null != (line = br.readLine())){
			upgradeDriver.withInput(new LongWritable(), new Text(line));
		}
		
		List<Pair<Text, IntWritable>> resultList = upgradeDriver.run();
		for(Pair<Text, IntWritable> p : resultList){
			System.out.println(p.getFirst().toString() + "," + p.getSecond());
		}
	}

}
