package test.guanka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuyForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.ItemBuySumReducer;
import net.digitcube.hadoop.tmp.LostPlayerGuankaLayoutMapper;
import net.digitcube.hadoop.tmp.LostPlayerGuankaLayoutReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GuanKaForPlayerTest {

	private MapReduceDriver<LongWritable, Text, Text, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> eventDriver;
	
	@Before
	public void setUp() {
		LostPlayerGuankaLayoutMapper map = new LostPlayerGuankaLayoutMapper();
		LostPlayerGuankaLayoutReducer reduce = new LostPlayerGuankaLayoutReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\tmp_guanka"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = eventDriver.run();
		
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			line = pair.getFirst().getSuffix() + "---" + pair.getFirst().toString();
			System.out.println(line);
		}
	}
}

