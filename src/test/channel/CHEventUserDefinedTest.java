package test.channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.mapreduce.channel.CHErrorReportMapper;
import net.digitcube.hadoop.mapreduce.channel.CHErrorReportReducer;
import net.digitcube.hadoop.mapreduce.channel.event.CHEventUserDefinedMapper;
import net.digitcube.hadoop.mapreduce.channel.event.CHEventUserDefinedReducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class CHEventUserDefinedTest {

	private MapReduceDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> mapReduceDriver;
	private MapDriver<LongWritable, Text, OutFieldsBaseModel, OutFieldsBaseModel> mapDriver;
	
	@Before
	public void setUp() {
		CHEventUserDefinedMapper map = new CHEventUserDefinedMapper();
		CHEventUserDefinedReducer reduce = new CHEventUserDefinedReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map, reduce);
		mapDriver = MapDriver.newMapDriver(map);
	}
	
	@Test
	public void testMap() throws Exception {
		System.out.println("Map,Begin------------------------------------------------------------------");
		// 多个输入源
		BufferedReader[] arr = {
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_EventUserDefined"))};
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = mapDriver.run();
		Collections.sort(onlineList, new Comparator<Pair<OutFieldsBaseModel,OutFieldsBaseModel>>() {
			@Override
			public int compare(Pair<OutFieldsBaseModel, OutFieldsBaseModel> o1,
					Pair<OutFieldsBaseModel, OutFieldsBaseModel> o2) {
				if(o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix())>0){
					return -1;
				}
				return 0;
			}
		});
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString() + "\t"
					+ pair.getSecond().getSuffix();
			System.out.println(lin);
		}
		System.out.println("Map,End------------------------------------------------------------------");
	}

	@Test
	public void testMapReduce() throws Exception {
		System.out.println("MapReduce,Begin------------------------------------------------------------------");
		// 多个输入源
		BufferedReader[] arr = {
				new BufferedReader(new FileReader("D:\\hadoop\\part-r-00001-DESelf_EventUserDefined"))};
		LongWritable longWritable = new LongWritable();
		String line = null;
		for (BufferedReader br : arr) {
			while (null != (line = br.readLine())) {
				mapReduceDriver.withInput(longWritable, new Text(line));
			}
			br.close();
		}
		List<Pair<OutFieldsBaseModel, OutFieldsBaseModel>> onlineList = mapReduceDriver.run();
		Collections.sort(onlineList, new Comparator<Pair<OutFieldsBaseModel,OutFieldsBaseModel>>() {
			@Override
			public int compare(Pair<OutFieldsBaseModel, OutFieldsBaseModel> o1,
					Pair<OutFieldsBaseModel, OutFieldsBaseModel> o2) {
				if(o1.getFirst().getSuffix().compareTo(o2.getFirst().getSuffix())>0){
					return -1;
				}
				return 0;
			}
		});
		for(Pair<OutFieldsBaseModel, OutFieldsBaseModel> pair : onlineList){
			String lin = pair.getFirst().toString() + "\t" + pair.getSecond().toString() + "\t"
					+ pair.getSecond().getSuffix();
			System.out.println(lin);
		}
		System.out.println("MapReduce,End------------------------------------------------------------------");
	}
}

