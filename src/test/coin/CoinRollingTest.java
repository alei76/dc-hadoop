package test.coin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.coin.CoinRollingDayMapper;
import net.digitcube.hadoop.mapreduce.coin.CoinRollingDayReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumReducer;
import net.digitcube.hadoop.model.EventLog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class CoinRollingTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	
	@Before
	public void setUp() {
		CoinRollingDayMapper map = new CoinRollingDayMapper();
		CoinRollingDayReducer reduce = new CoinRollingDayReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\coinNum.0401"));
		String line = null;
		while(null != (line = br.readLine())){
			String[] array = line.split(MRConstants.SEPERATOR_IN);
			
			EventLog eventLog = new EventLog(array);
			String[] appInfo = eventLog.getAppID().split("\\|");
			String accountID = eventLog.getAccountID();
			String platform = eventLog.getPlatform();
			String channel = eventLog.getChannel();
			String gameServer = eventLog.getGameServer();
			String seq = eventLog.getArrtMap().get("seq");
			String coinNum = eventLog.getArrtMap().get("total");
			if(null == accountID || null == platform || null == channel || null == gameServer || 
					null == seq || null == coinNum){
				System.out.println(line);
			}
		}
		br.close();
		
	}
}

