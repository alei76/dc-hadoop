package test.taskanditem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaForPlayerMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaForPlayerReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.GuanKaStatReducer;
import net.digitcube.hadoop.mapreduce.taskanditem.TaskStatMapper;
import net.digitcube.hadoop.mapreduce.taskanditem.TaskStatReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class GuanKaTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver2;
	
	@Before
	public void setUp() {
		GuanKaForPlayerMapper map = new GuanKaForPlayerMapper();
		GuanKaForPlayerReducer reduce = new GuanKaForPlayerReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		GuanKaStatMapper m = new GuanKaStatMapper();
		GuanKaStatReducer r = new GuanKaStatReducer();
		eventDriver2 = MapReduceDriver.newMapReduceDriver(m, r);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\guka_test.0402"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver.withInput(longWritable, new Text(line));
		}
		
		//job1
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = eventDriver.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			eventDriver2.withInput(longWritable, new Text(line));
			System.out.println(line);
		}
		
		System.out.println("\n\n------------------------------------------------------");
		//job2
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> List = eventDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : List){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
			eventDriver2.withInput(longWritable, new Text(line));
			System.out.println(line);
		}
	}
}

