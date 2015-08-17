package test.coin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.coin.CoinLayoutMapper;
import net.digitcube.hadoop.mapreduce.coin.CoinLayoutReducer;
import net.digitcube.hadoop.mapreduce.coin.CoinUserDureMapper;
import net.digitcube.hadoop.mapreduce.coin.CoinUserDureReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class CoinLostUserNumTest {
	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, NullWritable,  OutFieldsBaseModel, NullWritable> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, Text, OutFieldsBaseModel, LongWritable> eventDriver2;
	
	@Before
	public void setUp() {
		CoinUserDureMapper map = new CoinUserDureMapper();
		CoinUserDureReducer reduce = new CoinUserDureReducer();
		eventDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		
		CoinLayoutMapper map2 = new CoinLayoutMapper();
		CoinLayoutReducer reduce2 = new CoinLayoutReducer();
		//eventDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
	}

	@Test
	public void testCoinLostUserNum() throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("C:/Users/Administrator/Desktop/coing.1215"));
		LongWritable longWritable = new LongWritable();
		String line = null;
		while(null != (line = br.readLine())){
			eventDriver2.withInput(longWritable, new Text(line));
		}
		
		System.out.println("\n\n--------------------------------------------------------\n\n");
		List<Pair<OutFieldsBaseModel, LongWritable>> list = eventDriver2.run();
		for(Pair<OutFieldsBaseModel, LongWritable> pair : list){
			System.out.println(pair.getFirst().toString() + "\t" + pair.getSecond().toString());
		}
	}

}
