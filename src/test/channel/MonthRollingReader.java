package test.channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHAppUseHabitMapper;
import net.digitcube.hadoop.mapreduce.channel.CHAppUseHabitReducer;
import net.digitcube.hadoop.mapreduce.channel.week.CHUserInfoRollingWeekMapper;
import net.digitcube.hadoop.mapreduce.channel.week.CHUserInfoRollingWeekReducer;
import net.digitcube.hadoop.mapreduce.channel.week.CHWeekUserHabitsMapper;
import net.digitcube.hadoop.mapreduce.channel.week.CHWeekUserHabitsReducer;
import net.digitcube.hadoop.model.channel.UserInfoMonthRolling2;
import net.digitcube.hadoop.model.channel.UserInfoWeekRolling2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class MonthRollingReader {

	@Before
	public void setUp() {
	}
	
	@Test
	public void test() throws Exception {
		// 多个输入源
		BufferedReader[] arr = {
				new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\part-r-00000-CHANNEL_ROLLING_MONTH"))/*,
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_App_Uninstall")),
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_App_Launch"))*/};
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				UserInfoMonthRolling2 userInfoWeekRolling = new UserInfoMonthRolling2(new Date(), line.split("\t"));
				System.out.println(userInfoWeekRolling);
			}
			br.close();
		}
	}
}

