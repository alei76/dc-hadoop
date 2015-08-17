package net.digitcube.hadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import net.digitcube.hadoop.mapreduce.hbase.PlayerBasicInfo4HBaseRolling2Mapper;

public class BasicInfo4HBaseDriver {

	public static void main(String[] args) throws Exception {
		basicInfo4HBase(args[0], args[1]);
	}


	private static void basicInfo4HBase(String inputPath, String outputPath) throws Exception {
		System.out.println("start...");
        Configuration conf = new Configuration();
        conf.set("tmpjars", "file:///home/hadoop/hbase-0.94.16/lib/guava-11.0.2.jar");
        Job job = new Job(conf, "BasicInfo4HBase");
        job.setJarByClass(BasicInfo4HBaseDriver.class);
        job.setMapperClass(PlayerBasicInfo4HBaseRolling2Mapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        Configuration hbaseConf = HBaseConfiguration.create();
        HTable table =new HTable(hbaseConf, "player_basic_info");
        HFileOutputFormat.configureIncrementalLoad(job, table);
        
        if(job.waitForCompletion(true)) {
        	System.out.println("mr success...");
	        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
	        loader.doBulkLoad(new Path(outputPath), table);
        }else{
        	System.out.println("mr failed...");
        }
        System.out.println("end...");
	}
}
