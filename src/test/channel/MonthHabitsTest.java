package test.channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHAppUseHabitMapper;
import net.digitcube.hadoop.mapreduce.channel.CHAppUseHabitReducer;
import net.digitcube.hadoop.mapreduce.channel.month.CHMonthUserHabitsMapper;
import net.digitcube.hadoop.mapreduce.channel.month.CHMonthUserHabitsReducer;
import net.digitcube.hadoop.mapreduce.channel.week.CHWeekUserHabitsMapper;
import net.digitcube.hadoop.mapreduce.channel.week.CHWeekUserHabitsReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class MonthHabitsTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;
	
	@Before
	public void setUp() {
		CHMonthUserHabitsMapper map = new CHMonthUserHabitsMapper();
		CHMonthUserHabitsReducer reduce = new CHMonthUserHabitsReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
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
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = mapReduceDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(lin);
		}
	}
}

