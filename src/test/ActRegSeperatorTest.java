package test;

import java.util.List;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.ActRegSeparateMapper;

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

public class ActRegSeperatorTest {

	private Mapper<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> mapper;
	private MapDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> mapdriver;
	@Before
	public void setUp() {
		mapper = new ActRegSeparateMapper();
		mapdriver = new MapDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel>();
		mapdriver.setMapper(mapper);
	}

	@Test
	public void testIdentityMapper() throws Exception {

		List<String> userInfoList = TestDataUtil.generateUserInfoData(60, 1);
		for(String userInfo : userInfoList){
			mapdriver.withInput(new LongWritable(), new Text(userInfo));
		}
		
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> resultList = mapdriver.run();
		int regNum = 0;
		int actNum = 0;
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> p : resultList){
			if(Constants.SUFFIX_REG.equals(p.getFirst().getSuffix())){
				regNum++;
			}else{
				actNum++;
			}
			System.out.println(p.getFirst().toString());
		}
		
		System.out.println("regNum = " + regNum);
		System.out.println("actNum = " + actNum);
		
	}

}
