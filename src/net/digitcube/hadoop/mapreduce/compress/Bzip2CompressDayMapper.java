package net.digitcube.hadoop.mapreduce.compress;

import java.io.IOException;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 主要逻辑：
 * 输入一天的注册、在线、付费、自定义时间等原始日志
 * 经过 MR 处理后以相应的文件名为后缀压缩输出
 * 
 * 该  MR 主要功能是压缩集群上历史数据（最原始数据，从 logserver 上传），并冷存起来
 * 下面列表是几个常用的压缩算法的压缩比及速度：
 * 压缩算法	原始文件大小	压缩后的文件大小	压缩速度		解压缩速度	可分割性
 * gzip　　	8.3GB　　	1.8GB			17.5MB/s	58MB/s		 不
 * bzip2	8.3GB		1.1GB			2.4MB/s		9.5MB/s		是
 * LZO-bset	8.3GB		2GB				4MB/s		60.6MB/s	是
 * LZO		8.3GB		2.9GB			49.3MB/S	74.6MB/s	是
 * 
 * 结合业务需求考虑，这里选用压缩比最大的  bzip2 压缩算法
 * 选择这种算法有利有弊：
 * 利：该压缩算法压缩比最大，利于我们存储更多历史数据
 * 弊：该算法压缩和解压速度是最慢的
 * 这里认为历史数据压缩存储后，将很少再拿出来计算
 * 所以认为该压缩解压速度可以接受
 */
public class Bzip2CompressDayMapper extends Mapper<LongWritable, Text, Text, BigFieldsBaseModel> {

	private Text keyObj = new Text();
	private BigFieldsBaseModel valObj = new BigFieldsBaseModel(new String[1]);
	
	private String suffix = "";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// path-->/data/logserver/online/2015/02/02/00/input/Online-2015-02-02-00.log_dcserver1
		Path path = ((FileSplit)context.getInputSplit()).getPath();
		// fileName-->Online-2015-02-02-00.log_dcserver1
		String fileName = path.getName();
		// suffix-->Online
		suffix = fileName.split("-")[0];
		keyObj.set(suffix);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		valObj.getOutFields()[0] = value.toString();
		context.write(keyObj, valObj);
	}
	
	public static void main(String[] args){
		String s = "/data/logserver/online/2015/02/02/00/input/Online-2015-02-02-00.log_dcserver1";
		Path path = new Path(s);
		System.out.println(path.getName());
		System.out.println(path.getName().split("-")[0]);
	}
}
