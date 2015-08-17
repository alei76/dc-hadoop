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

public class DownloadForPlayerTest {

	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> downloadDriver;
	
	@Before
	public void setUp() {
		//download for player
		CHDownloadForPlayerMapper downloadMap = new CHDownloadForPlayerMapper();
		CHDownloadForPlayerReducer downloadReduce = new CHDownloadForPlayerReducer();
		downloadDriver = MapReduceDriver.newMapReduceDriver(downloadMap, downloadReduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		LongWritable longWritable = new LongWritable();
		String line = null;
		
		//download
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\part-r-00000-DESelf_Channel_Res_Download")));
		while(null != (line=br.readLine())){
			downloadDriver.withInput(longWritable, new Text(line));
		}
		br.close();
		
		//online
		br = new BufferedReader(new InputStreamReader(new FileInputStream("C:\\Users\\Administrator\\Desktop\\online")));
		while(null != (line=br.readLine())){
			downloadDriver.withInput(longWritable, new Text(line));
		}
		br.close();
				
		List<Pair<OutFieldsBaseModel, NullWritable>> downloadList = downloadDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : downloadList){
			String result = pair.getFirst().toString();
			System.out.println(pair.getFirst().getSuffix()+"---"+result);
			//gameCenterDriver.withInput(longWritable, new Text(result));
		}
	}
}

