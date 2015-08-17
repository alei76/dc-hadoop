package test.tag;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.tag.TagInfoRollingMapper;
import net.digitcube.hadoop.mapreduce.tag.TagInfoRollingReducer;
import net.digitcube.hadoop.mapreduce.tag.TagLostAndRetainMapper;
import net.digitcube.hadoop.mapreduce.tag.TagLostAndRetainReducer;
import net.digitcube.hadoop.mapreduce.tag.TagOnlineAndPayMapper;
import net.digitcube.hadoop.mapreduce.tag.TagOnlineAndPayReducer;
import net.digitcube.hadoop.mapreduce.tag.TagStayAddRmMapper;
import net.digitcube.hadoop.mapreduce.tag.TagStayAddRmReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.TaskStatMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.TaskStatReducer;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingDayMapper;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingDayReducer;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingHourMapper;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingHourReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TagRollingTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, BigFieldsBaseModel, OutFieldsBaseModel, NullWritable> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, BigFieldsBaseModel, OutFieldsBaseModel, NullWritable> eventDriver2;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver3;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver33;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver4;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver44;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> eventDriver5;
	
	@Before
	public void setUp() {
		TagInfoRollingMapper map = new TagInfoRollingMapper();
		TagInfoRollingReducer reduce = new TagInfoRollingReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		TagInfoRollingMapper map2 = new TagInfoRollingMapper();
		TagInfoRollingReducer reduce2 = new TagInfoRollingReducer();
		eventDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
		
		TagStayAddRmMapper map3 = new TagStayAddRmMapper();
		TagStayAddRmReducer reduce3 = new TagStayAddRmReducer();
		eventDriver3 = MapReduceDriver.newMapReduceDriver(map3, reduce3);
		TagStayAddRmMapper map33 = new TagStayAddRmMapper();
		TagStayAddRmReducer reduce33 = new TagStayAddRmReducer();
		eventDriver33 = MapReduceDriver.newMapReduceDriver(map33, reduce33);
		
		TagOnlineAndPayMapper map4 = new TagOnlineAndPayMapper();
		TagOnlineAndPayReducer reduce4 = new TagOnlineAndPayReducer();
		eventDriver4 = MapReduceDriver.newMapReduceDriver(map4, reduce4);
		TagOnlineAndPayMapper map44 = new TagOnlineAndPayMapper();
		TagOnlineAndPayReducer reduce44 = new TagOnlineAndPayReducer();
		eventDriver44 = MapReduceDriver.newMapReduceDriver(map44, reduce44);
		
		TagLostAndRetainMapper map5 = new TagLostAndRetainMapper();
		TagLostAndRetainReducer reduce5 = new TagLostAndRetainReducer();
		eventDriver5 = MapReduceDriver.newMapReduceDriver(map5, reduce5);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		//step 1
		System.out.println("step1-------------------------");
		String line = null;
		LongWritable longWritable = new LongWritable();
		//标签
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\DESelf_addTag"));
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		//在线
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\ONLINE_DAY"));
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		//付费
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\PAYMENT_DAY"));
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = eventDriver.run();
		List<Pair<OutFieldsBaseModel, NullWritable>> rollList = new ArrayList<Pair<OutFieldsBaseModel, NullWritable>>();
		/*for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			//System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString());
			if("TAG_ROLLING_DAY".equals(pair.getFirst().getSuffix())){
				rollList.add(pair);
			}
			if("TAG_STAY_ADD_RM".equals(pair.getFirst().getSuffix())){
				eventDriver3.withInput(longWritable, new Text(pair.getFirst().toString()));
			}
			if("TAG_ONLINE_DAY".equals(pair.getFirst().getSuffix())){
				eventDriver4.withInput(longWritable, new Text(pair.getFirst().toString() + "\tTAG_ONLINE_DAY"));
			}
			if("TAG_PAYMENT_DAY".equals(pair.getFirst().getSuffix())){
				eventDriver4.withInput(longWritable, new Text(pair.getFirst().toString() + "\tTAG_PAYMENT_DAY"));
			}
		}
		
		System.out.println("tag stay add remove-------------------------");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> tagInfoList = eventDriver3.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : tagInfoList){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
		System.out.println("tag online pay info-------------------------");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> tagOnlineList = eventDriver4.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : tagOnlineList){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}*/
		
		//step2
		System.out.println("step2-------------------------");
		//滚存
		for(Pair<OutFieldsBaseModel, NullWritable> pair : rollList){
			//System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString());
			eventDriver2.withInput(longWritable, new Text(pair.getFirst().toString() + "\tTAG_ROLLING_DAY")); 
		}
		//标签
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\2_DESelf_addTag"));
		while(null != (line = br.readLine())){
			eventDriver2.withInput(longWritable, new Text(line));
		}
		br.close();
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\DESelf_removeTag.txt"));
		while(null != (line = br.readLine())){
			eventDriver2.withInput(longWritable, new Text(line));
		}
		br.close();
		
		//在线
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\ONLINE_DAY"));
		while(null != (line = br.readLine())){
			eventDriver2.withInput(longWritable, new Text(line));
		}
		br.close();
		//付费
		br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\PAYMENT_DAY"));
		while(null != (line = br.readLine())){
			eventDriver2.withInput(longWritable, new Text(line));
		}
		br.close();
				
		rollList = eventDriver2.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : rollList){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString());
			if("TAG_ROLLING_DAY".equals(pair.getFirst().getSuffix())){
				System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString());
			}
			if("TAG_STAY_ADD_RM".equals(pair.getFirst().getSuffix())){
				eventDriver33.withInput(longWritable, new Text(pair.getFirst().toString()));
			}
			if("TAG_ONLINE_DAY".equals(pair.getFirst().getSuffix())){
				eventDriver44.withInput(longWritable, new Text(pair.getFirst().toString() + "\tTAG_ONLINE_DAY"));
			}
			if("TAG_PAYMENT_DAY".equals(pair.getFirst().getSuffix())){
				eventDriver44.withInput(longWritable, new Text(pair.getFirst().toString() + "\tTAG_PAYMENT_DAY"));
			}
			if("TAG_PLAYER_RETAIN".equals(pair.getFirst().getSuffix())){
				eventDriver5.withInput(longWritable, new Text(pair.getFirst().toString() + "\tTAG_PLAYER_RETAIN"));
			}
		}
		System.out.println("tag stay add remove-------------------------");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> tagInfoList33 = eventDriver33.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : tagInfoList33){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
		System.out.println("tag online pay info-------------------------");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> tagOnlineList44 = eventDriver44.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : tagOnlineList44){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
		System.out.println("tag lost retain info-------------------------");
		List<Pair<OutFieldsBaseModel, IntWritable>> lostRetainList = eventDriver5.run();
		for(Pair<OutFieldsBaseModel, IntWritable> pair : lostRetainList){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
	}
}

