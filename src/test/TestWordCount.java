package test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

public class TestWordCount {

	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WordCountMapper map = new WordCountMapper();
		WordCountReducer red = new WordCountReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, red);
	}

	@Test
	public void testIdentityMapper() throws Exception {
		List<Pair<Text, IntWritable>> wordCountList = mapReduceDriver.withInput(new LongWritable(), new Text("a bc"))
															   .withInput(new LongWritable(), new Text("a d"))
															   .withInput(new LongWritable(), new Text("a a e"))
															   .run();
		
		for(Pair<Text, IntWritable> pair : wordCountList){
			System.out.println(pair.getFirst().toString() + " : " + pair.getSecond().get());
		}
	}

	static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			System.out.println(key.toString() + " : " + sum);
		}
	}
	
	public static void main(String[] args) throws Exception{
		/*System.out.println("start...");
		int count = 100000;
		BufferedWriter bw = new BufferedWriter(new FileWriter("C:\\Users\\Administrator\\Desktop\\accountSet."+count)); 
		for(int i=0; i<count; i++){
			bw.write("92C099A82CAAE71E3CB6CD48D9030067,");
		}
		bw.close();
		System.out.println("end...");*/
		
		/*Map<Integer, Integer> loginTimeMap = new HashMap<Integer, Integer>();
		int i = 0;
		loginTimeMap.put(i++, i++);
		loginTimeMap.put(i++, i++);
		loginTimeMap.put(i++, i++);
		loginTimeMap.put(i++, i++);
		loginTimeMap.put(i++, i++);
		Gson gson = new Gson();
		String str = getJsonStr(loginTimeMap);
		
		Map<Integer, Integer> amap = gson.fromJson(str, new HashMap<Integer,Integer>().getClass());
		System.out.println("gson="+str);
		System.out.println("loginTimeMap="+loginTimeMap);
		System.out.println("amap="+amap);
		
		Set<String> set = new HashSet<String>();
		set.add(i++ + "");
		set.add(i++ + "");
		set.add(i++ + "");
		set.add(i++ + "");
		str = getJsonStr(set);
		System.out.println("gson="+str);
		Set<String> s = gson.fromJson(str, HashSet.class);
		System.out.println(s);*/
		String s = "BFEBF03470D1C2EC462D23A3DAC78BF%7C2.92_2-r-00023";
		System.out.println(s.contains(":"));
	}
	
	private static String getJsonStr(Object o){
		Gson gson = new Gson();
		String str = gson.toJson(o);
		return str;
	}
}
