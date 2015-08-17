package test.ext;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.ext.roll.ExtInfoRollingDayMapper;
import net.digitcube.hadoop.mapreduce.ext.roll.ExtInfoRollingDayReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class ExtInfoRollingTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> rollDriver;
	
	@Before
	public void setUp() {
		ExtInfoRollingDayMapper map = new ExtInfoRollingDayMapper();
		ExtInfoRollingDayReducer reduce = new ExtInfoRollingDayReducer();
//		rollDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

		LongWritable longWritable = new LongWritable();
		
		/*BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\item.0421"));
		String line = null;
		while(null != (line = br.readLine())){
			rollDriver.withInput(longWritable, new Text(line));
		}*/
		
		String line = "00007E2ECD0561179A60BACA19E0AC86	1.0	2	ID001	_ALL_GS	007E8FEDF054526125200061874CBF2F	007E8FEDF054526125200061874CBF2F	H4sIAAAAAAAAAKtWMjQxsjQ3tDAxMFCyqlYyMDAEUZklqbmeKUpWYL4OmBtSWZAKFHi6f_XLhvnP5i991rkPKJNUWumcX5pXomRlrKOUnloC5RjoKJUWp8I4tTpAc4zQzDVCNfdle--zaRueTd3yfEIbSeYao5lrjGru86YdT3Y0PF2_E8VQQzyG1tYCAHrGJoYVAQAA	H4sIAAAAAAAAAKtWMjQxsjQ3tDAxMFCyqlZ6vmaNkeXT1s0gdnppYp53omeKkhVCWEcpKTU9My8kMze1WMnKWEepuDQ5ObW4GCpgqKOUlpiZU1qUChUwqtUBaTY2wmomRJhMM40ssLvTgggzDdDNNIa60wC7Ow0wzTQk7FBDmO8NsZtqiMVUgoYaQM00xm6mMaaZFgR9b15bWwsAF0QnIwoCAAA.	H4sIAAAAAAAAAKuuBQBDv6ajAgAAAA..";
		rollDriver.withInput(longWritable, new Text(line));
		//job1
		List<Pair<OutFieldsBaseModel, NullWritable>> onlineList = rollDriver.run();
		for(Pair<OutFieldsBaseModel, NullWritable> pair : onlineList){
			System.out.println(pair.getFirst().getSuffix() + "---" + pair.getFirst().toString());
		}
	}
}

