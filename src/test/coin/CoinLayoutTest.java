package test.coin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class CoinLayoutTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver2;
	
	@Before
	public void setUp() {
		ItemBuyForPlayerMapper map = new ItemBuyForPlayerMapper();
		ItemBuyForPlayerReducer reduce = new ItemBuyForPlayerReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		ItemBuySumMapper map2 = new ItemBuySumMapper();
		ItemBuySumReducer reduce2 = new ItemBuySumReducer();
		eventDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\event.log"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = eventDriver.run();
		
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(line);
			eventDriver2.withInput(longWritable, new Text(line));
		}
		System.out.println("\n\n--------------------------------------------------------\n\n");
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> list = eventDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : list){
			System.out.println(pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
	}
}

