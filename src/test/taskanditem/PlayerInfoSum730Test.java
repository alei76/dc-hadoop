package test.taskanditem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.taskanditem.PlayerInfoSum730Mapper;
import net.digitcube.hadoop.mapreduce.taskanditem.PlayerInfoSum730Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class PlayerInfoSum730Test {
	
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver;
	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> eventDriver2;
	
	@Before
	public void setUp() {		
		PlayerInfoSum730Mapper map2 = new PlayerInfoSum730Mapper();
		PlayerInfoSum730Reducer reduce2 = new PlayerInfoSum730Reducer();
		eventDriver2 = MapReduceDriver.newMapReduceDriver(map2, reduce2);
	}

	@Test
	public void testAcuPcuCntHour() throws Exception {

//		BufferedReader br = new BufferedReader(new FileReader("F://uuu"));
		LongWritable longWritable = new LongWritable();
//		String line = null;
//		
//		while(null != (line = br.readLine())){
//			eventDriver2.withInput(longWritable, new Text(line));
//		}
		
		String line = "4FCF7B45C846D23C5206FBB62C0687AE|1.0.0	2	_ALL_GS	1_1_125309	UserLenovo	O	NO	H4sIAAAAAAAAAONgYGQKSbnaIMXJwMvFxmZmAATGYmxPexc-72zXMWD0qWBgZGMPCHONNzawEGDkhqkxRKgRAaphYuN_vnres66Gp3N2PV067WnXRgFWNp5n6_ufTV_6clHb0_5pAvwgvRYGpoYgvc9nrXvRtARmPsezrr1A0-INIRYYAo23ADriZePkp63bdQyYwIrYnnVMeLphowATXIkRQgkzWAmqhcxQhQYmFgiFEAsxHAu21xToOENDdM9jOs7A0BzdQNQQMjUwtjTECEWYQUYQRRZARbiDwghhm4EJftsMofGBqobn6cS9L_ZPADuhB0mhKR7DJHk0ebgB8dqlWxICAAA.";
		eventDriver2.withInput(longWritable, new Text(line));
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = eventDriver2.run();
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			line = pair.getFirst().toString() + "\t" + pair.getSecond().toString();
//			if(pair.getFirst().getSuffix().equals("Item_Sys_Output_Num")){
//				System.out.println(pair.getFirst().getSuffix() + ":" + line);
//			}
			System.out.println(pair.getFirst().getSuffix() + ":" + line);
			
		}
	}

}
