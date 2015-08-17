package test.taskanditem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ExtendInfoDetail;
import net.digitcube.hadoop.jce.UserExtendInfoLog.ItemDayInfo;
import net.digitcube.hadoop.mapreduce.userroll.UserExtInfoRollingDayMap;
import net.digitcube.hadoop.mapreduce.userroll.UserExtInfoRollingDayReducer;
import net.digitcube.hadoop.model.UserExtInfoRollingLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class UserRollingExtTest {
	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> eventDriver2;
	
	@Before
	public void setUp() {		
		UserExtInfoRollingDayMap map2 = new UserExtInfoRollingDayMap();
		UserExtInfoRollingDayReducer reduce2 = new UserExtInfoRollingDayReducer();
		eventDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:/Users/Administrator/Desktop/head"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		
		int totalcount = 0;
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		while(null != (line = br.readLine())){
			String[] arr = line.split("\t");
			System.out.println("Newline-->"+arr[0] + "-->" + arr[4]);
			UserExtInfoRollingLog extInfoRollingLog = new UserExtInfoRollingLog(arr);
			/*if(!"_ALL_GS".equals(extInfoRollingLog.getGameServer())){
				continue;
			}*/
			Map<Integer, ExtendInfoDetail> map = extInfoRollingLog.getDetailMap().getDetailMap();
			Set<Integer> keys = map.keySet();
			for(Integer key : keys){
				ExtendInfoDetail info = map.get(key);
				if(info.itemDayInfoList.isEmpty() && info.levelDayInfoList.isEmpty() && 
						info.taskDayInfoList.isEmpty()){
					cal.setTimeInMillis(1000L * key);
					System.out.println("empty-->"+sdf.format(cal.getTime()));
				}else{
					cal.setTimeInMillis(1000L * key);
					System.out.println("ntemp-->"+sdf.format(cal.getTime()));
				}
				/*System.out.println("----------------------"+key);
				for(ItemDayInfo day : info.getItemDayInfoList()){
					for(Entry<String, Integer> entry : day.getBuyCntMap().entrySet()){
						System.out.println(entry);
					}
				}*/
			}
		}
		
		System.out.println(totalcount);
	}

}
