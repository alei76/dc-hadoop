package test.guanka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatReducer;
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

public class GuanKaStatTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	
	@Before
	public void setUp() {
		GuanKaStatMapper map = new GuanKaStatMapper();
		GuanKaStatReducer reduce = new GuanKaStatReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\GUANKA_FOR_PLAYER"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = eventDriver.run();
		
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			System.out.println(line);
		}
	}
}

