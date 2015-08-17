package test.channel;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHDownloadForPlayerMapper;
import net.digitcube.hadoop.mapreduce.channel.CHDownloadForPlayerReducer;
import net.digitcube.hadoop.mapreduce.channel.CHGameCenterMapper;
import net.digitcube.hadoop.mapreduce.channel.CHGameCenterReducer;
import net.digitcube.hadoop.mapreduce.channel.CHOnlineDayMapper;
import net.digitcube.hadoop.mapreduce.channel.CHOnlineDayReducer;
import net.digitcube.hadoop.mapreduce.channel.CHRetainAndFreshMapper;
import net.digitcube.hadoop.mapreduce.channel.CHRetainAndFreshReducer;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoLayoutMapper;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoLayoutReducer;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoMapper;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoReducer;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoRollingDayMapper;
import net.digitcube.hadoop.mapreduce.channel.CHUserInfoRollingDayReducer;

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class CHInfoRollingTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> infoDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> onlineDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> rollingDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> layoutDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> retainDriver;
	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> onlineDriver2;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> rollingDriver2;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, IntWritable> retainDriver2;
	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> downloadDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> gameCenterDriver;
	
	@Before
	public void setUp() {
		CHUserInfoMapper infoMap = new CHUserInfoMapper();
		CHUserInfoReducer infoReduce = new CHUserInfoReducer();
		infoDriver = MapReduceDriver.newMapReduceDriver(infoMap, infoReduce);
		//online
		CHOnlineDayMapper onlineMap = new CHOnlineDayMapper();
		CHOnlineDayReducer onlineReduce = new CHOnlineDayReducer();
		onlineDriver = MapReduceDriver.newMapReduceDriver(onlineMap, onlineReduce);
		//rolling
		CHUserInfoRollingDayMapper rollingMap = new CHUserInfoRollingDayMapper();
		CHUserInfoRollingDayReducer rollingReduce = new CHUserInfoRollingDayReducer();
		rollingDriver = MapReduceDriver.newMapReduceDriver(rollingMap, rollingReduce);
		
		//UserInfoLayout
		CHUserInfoLayoutMapper layoutMap = new CHUserInfoLayoutMapper();
		CHUserInfoLayoutReducer layoutReduce = new CHUserInfoLayoutReducer();
		layoutDriver = MapReduceDriver.newMapReduceDriver(layoutMap, layoutReduce);
		//Retain
		CHRetainAndFreshMapper retainMap = new CHRetainAndFreshMapper();
		CHRetainAndFreshReducer retainReduce = new CHRetainAndFreshReducer();
		retainDriver = MapReduceDriver.newMapReduceDriver(retainMap, retainReduce);
		
		//online2
		CHOnlineDayMapper onlineMap2 = new CHOnlineDayMapper();
		CHOnlineDayReducer onlineReduce2 = new CHOnlineDayReducer();
		onlineDriver2 = MapReduceDriver.newMapReduceDriver(onlineMap2, onlineReduce2);
		//rolling2
		CHUserInfoRollingDayMapper rollingMap2 = new CHUserInfoRollingDayMapper();
		CHUserInfoRollingDayReducer rollingReduce2 = new CHUserInfoRollingDayReducer();
		rollingDriver2 = MapReduceDriver.newMapReduceDriver(rollingMap2, rollingReduce2);
		//retain2
		CHRetainAndFreshMapper retainMap2 = new CHRetainAndFreshMapper();
		CHRetainAndFreshReducer retainReduce2 = new CHRetainAndFreshReducer();
		retainDriver2 = MapReduceDriver.newMapReduceDriver(retainMap2, retainReduce2);
		
		//download for player
		CHDownloadForPlayerMapper downloadMap = new CHDownloadForPlayerMapper();
		CHDownloadForPlayerReducer downloadReduce = new CHDownloadForPlayerReducer();
		downloadDriver = MapReduceDriver.newMapReduceDriver(downloadMap, downloadReduce);
		
		//game center
		CHGameCenterMapper gcMap = new CHGameCenterMapper();
		CHGameCenterReducer gcReduce = new CHGameCenterReducer();
		gameCenterDriver = MapReduceDriver.newMapReduceDriver(gcMap, gcReduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		LongWritable longWritable = new LongWritable();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\ch_info.txt")));
		String line = null;
		while(null != (line=br.readLine())){
			String[] array = line.split(MRConstants.SEPERATOR_IN);
			infoDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, NullWritable>> infoList = infoDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : infoList){
			String result = pair.getFirst().toString();
			//System.out.println(result);
			rollingDriver.withInput(longWritable, new Text(result));
		}
		
		//online
		br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\ch_online.txt")));
		while(null != (line=br.readLine())){
			String[] array = line.split(MRConstants.SEPERATOR_IN);
			onlineDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = onlineDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			String result = pair.getFirst().toString();
			//System.out.println(result);
			rollingDriver.withInput(longWritable, new Text(result));
		}
		
		//roll
		rollingDriver.getConfiguration().set("job.schedule.time", "2015-03-14 06:30:00");
		List<Pair<OutFieldsBaseModel, NullWritable>> rollingList = rollingDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : rollingList){
			String result = pair.getFirst().toString();
			//System.out.println(pair.getFirst().getSuffix() + "---" + result);
			/*if(Constants.SUFFIX_CHANNEL_ONLINE_INFO.equals(pair.getFirst().getSuffix())){
				layoutDriver.withInput(longWritable, new Text(result));
			}*/
			
			/*if(Constants.SUFFIX_CHANNEL_RETAIN.equals(pair.getFirst().getSuffix())){
				retainDriver.withInput(longWritable, new Text(result));
			}*/
			
			if(Constants.SUFFIX_CHANNEL_ROLLING.equals(pair.getFirst().getSuffix())){
				//System.out.println(pair.getFirst().getSuffix() + "---" + result);
				rollingDriver2.withInput(longWritable, new Text(result));
			}
		}
		
		//UserInfoLayout
		/*List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> layoutList = layoutDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : layoutList){
			String result = pair.getFirst().toString();
			System.out.println(pair.getFirst().getSuffix()+"---"+pair.getFirst().toString() 
					+ "--"+pair.getSecond().toString());
		}*/
		
		//RetainAndRefresh
		/*retainDriver.getConfiguration().set("job.schedule.time", "2015-03-14 06:30:00");
		List<Pair<OutFieldsBaseModel, IntWritable>> retainList = retainDriver.run();
		for(Pair<OutFieldsBaseModel, IntWritable> pair : retainList){
			String result = pair.getFirst().toString();
			System.out.println(pair.getFirst().getSuffix()+"---"+pair.getFirst().toString() 
					+ "--"+pair.getSecond().toString());
		}*/
		
		//online day2
		br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\ch_online2.txt")));
		while(null != (line=br.readLine())){
			onlineDriver2.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList2 = onlineDriver2.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList2){
			String result = pair.getFirst().toString();
			//System.out.println(result);
			rollingDriver2.withInput(longWritable, new Text(result));
		}
		
		//rolling2
		rollingDriver2.getConfiguration().set("job.schedule.time", "2015-03-15 06:30:00");
		List<Pair<OutFieldsBaseModel, NullWritable>> rollingList2 = rollingDriver2.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : rollingList2){
			String result = pair.getFirst().toString();
			//System.out.println(pair.getFirst().getSuffix() + "---" + result);
			if(Constants.SUFFIX_CHANNEL_ONLINE_INFO.equals(pair.getFirst().getSuffix())){
				//layoutDriver.withInput(longWritable, new Text(result));
				downloadDriver.withInput(longWritable, new Text(result));
			}
			
			/*if(Constants.SUFFIX_CHANNEL_RETAIN.equals(pair.getFirst().getSuffix())){
				retainDriver2.withInput(longWritable, new Text(result));
			}*/
			
			if(Constants.SUFFIX_CHANNEL_ROLLING.equals(pair.getFirst().getSuffix())){
				System.out.println(pair.getFirst().getSuffix() + "---" + result);
				gameCenterDriver.withInput(longWritable, new Text(result));
			}
		}
		
		//RetainAndRefresh
		/*retainDriver2.getConfiguration().set("job.schedule.time", "2015-03-15 06:30:00");
		List<Pair<OutFieldsBaseModel, IntWritable>> retainList = retainDriver2.run();
		for(Pair<OutFieldsBaseModel, IntWritable> pair : retainList){
			String result = pair.getFirst().toString();
			System.out.println(pair.getFirst().getSuffix()+"---"+pair.getFirst().toString() 
					+ "--"+pair.getSecond().toString());
		}*/
		
		//download
		br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\ch_download.txt")));
		while(null != (line=br.readLine())){
			downloadDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		List<Pair<OutFieldsBaseModel, NullWritable>> downloadList = downloadDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : downloadList){
			String result = pair.getFirst().toString();
			System.out.println(pair.getFirst().getSuffix()+"---"+result);
			gameCenterDriver.withInput(longWritable, new Text(result));
		}
		
		//game center
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> gameCenterList = gameCenterDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : gameCenterList){
			String result = pair.getFirst().toString();
			System.out.println(pair.getFirst().getSuffix() + "---"+result+"--"+pair.getSecond());
		}
	}
}

