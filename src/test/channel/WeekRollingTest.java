package test.channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHAppUseHabitMapper;
import net.digitcube.hadoop.mapreduce.channel.CHAppUseHabitReducer;
import net.digitcube.hadoop.mapreduce.channel.week.CHUserInfoRollingWeekMapper;
import net.digitcube.hadoop.mapreduce.channel.week.CHUserInfoRollingWeekReducer;
import net.digitcube.hadoop.mapreduce.channel.week.CHWeekUserHabitsMapper;
import net.digitcube.hadoop.mapreduce.channel.week.CHWeekUserHabitsReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class WeekRollingTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> mapReduceDriver2;
	
	@Before
	public void setUp() {
		CHUserInfoRollingWeekMapper map = new CHUserInfoRollingWeekMapper();
		CHUserInfoRollingWeekReducer reduce = new CHUserInfoRollingWeekReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		CHUserInfoRollingWeekMapper map2 = new CHUserInfoRollingWeekMapper();
		CHUserInfoRollingWeekReducer reduce2 = new CHUserInfoRollingWeekReducer();
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
	}
	
	@Test
	public void test() throws Exception {
		// 多个输入源
		BufferedReader[] arr = {
				new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\week.txt"))/*,
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_App_Uninstall")),
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_App_Launch"))*/};
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			String lin = pair.getFirst().toString();
			if("CHANNEL_ROLLING_WEEK".equals(pair.getFirst().getSuffix())){
				System.out.println(pair.getFirst().getSuffix() + "---" + lin);
				mapReduceDriver2.withInput(longWritable, new Text(lin + "\t" + "CHANNEL_ROLLING_WEEK"));
			}
		}
		
		//---------------------------
		arr = new BufferedReader[]{
				new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\week2.txt"))/*,
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_App_Uninstall")),
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_App_Launch"))*/};
		
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriver2.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		System.out.println("-----------------------------------");
		onlineList = mapReduceDriver2.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			String lin = pair.getFirst().toString();
			if("CHANNEL_ROLLING_WEEK".equals(pair.getFirst().getSuffix())){
				System.out.println(pair.getFirst().getSuffix() + "---" + lin);
				mapReduceDriver2.withInput(longWritable, new Text(lin + "\t" + "CHANNEL_ROLLING_WEEK"));
			}
		}
		
		//---------------------------
		arr = new BufferedReader[]{
				new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\week3.txt"))/*,
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_App_Uninstall")),
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_App_Launch"))*/};
		
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriver2.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		System.out.println("-----------------------------------");
		onlineList = mapReduceDriver2.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			String lin = pair.getFirst().toString();
			if("CHANNEL_ROLLING_WEEK".equals(pair.getFirst().getSuffix())){
				System.out.println(pair.getFirst().getSuffix() + "---" + lin);
				mapReduceDriver2.withInput(longWritable, new Text(lin + "\t" + "CHANNEL_ROLLING_WEEK"));
			}
		}
	}
}

