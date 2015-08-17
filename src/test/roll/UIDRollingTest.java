package test.roll;

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

public class UIDRollingTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> upgradeDriver;
	@Before
	public void setUp() {
		UIDIncrementMapper map = new UIDIncrementMapper();
		UIDIncrementReducer reduce = new UIDIncrementReducer();
		upgradeDriver = new MapReduceDriver(map, reduce);
	}

	@Test
	public void testIdentityMapper() throws Exception {

		String s = "1395360000	3	BFEBF03470D1C82EC462D23A3DAC78BF|2.80	cf0620b2f390f45dad38960b2abb4e76	wangleiyuqin	2	634	1	1	26	zjh	480x800	4.0.4	三星GT-S7568	4	中国	北京市	中国联通	-	0	-	-	-	1395357183	2742	0";
		
		upgradeDriver.withInput(new LongWritable(), new Text(s));
		List<Pair<OutFieldsBaseModel, NullWritable>> resultList = upgradeDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> p : resultList){
			System.out.println(p.getFirst().toString());
		}
	}

}
