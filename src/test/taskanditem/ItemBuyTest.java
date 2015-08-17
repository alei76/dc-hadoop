package test.taskanditem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.AttrUtil;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.coin.CoinLayoutMapper;
import net.digitcube.hadoop.mapreduce.coin.CoinLayoutReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.item.ItemSumMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.item.ItemSumReducer;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class ItemBuyTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver2;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, IntWritable, OutFieldsBaseModel, LongWritable> eventDriver3;
	
	@Before
	public void setUp() {
		ItemBuyForPlayerMapper map = new ItemBuyForPlayerMapper();
		ItemBuyForPlayerReducer reduce = new ItemBuyForPlayerReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		ItemBuySumMapper m = new ItemBuySumMapper();
		ItemBuySumReducer r = new ItemBuySumReducer();
		eventDriver2 = MapReduceDriver.newMapReduceDriver(m, r);
		
		ItemSumMapper m3 = new ItemSumMapper();
		ItemSumReducer r3 = new ItemSumReducer();
		eventDriver3 = MapReduceDriver.newMapReduceDriver(m3, r3);
		
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {
		LongWritable longWritable = new LongWritable();
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\item_use"));
		String line = null;
		while(null != (line = br.readLine())){
			//eventDriver.withInput(longWritable, new Text(line));
			eventDriver3.withInput(longWritable, new Text(line));
		}
		br.close();
		
		/*//job1
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = eventDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			eventDriver2.withInput(longWritable, new Text(line));
			//System.out.println(line);
		}
		
		System.out.println("\n\n------------------------------------------------------");
		//job2
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> List = eventDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : List){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			//eventDriver2.withInput(longWritable, new Text(line));
			System.out.println(line);
		}*/
		
		List<Pair<OutFieldsBaseModel, LongWritable>> onlineList = eventDriver3.run();
		/*for(Pair<OutFieldsBaseModel, LongWritable> pair : onlineList){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(line);
		}*/
	}
}

