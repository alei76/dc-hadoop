package net.digitcube.hadoop.driver;

import java.util.Calendar;
import java.util.Date;

import net.digitcube.hadoop.common.BigFieldsBaseModel;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.SuffixMultipleOutputFormat;
import net.digitcube.hadoop.common.SuffixMultipleOutputFormat1;
import net.digitcube.hadoop.common.SuffixMultipleOutputFormat2;
import net.digitcube.hadoop.common.SuffixMultipleOutputFormat3;
import net.digitcube.hadoop.mapreduce.ActRegSeparateMapper;
//import net.digitcube.hadoop.mapreduce.NewPlayerLayoutMapper;
//import net.digitcube.hadoop.mapreduce.NewPlayerLayoutReducer;
import net.digitcube.hadoop.mapreduce.habits.UserHabitsMapper;
import net.digitcube.hadoop.mapreduce.habits.UserHabitsReducer;
import net.digitcube.hadoop.mapreduce.hbase.AdLabelInHbaseMapper;
import net.digitcube.hadoop.mapreduce.hbase.AdLabelInHbaseReducer;
import net.digitcube.hadoop.mapreduce.hbase.UserInfoRollingInHbaseMapper;
import net.digitcube.hadoop.mapreduce.hbase.UserInfoRollingInHbaseReducer;
import net.digitcube.hadoop.mapreduce.online.ACUHourForAppidMapper;
import net.digitcube.hadoop.mapreduce.online.ACUHourForAppidReducer;
import net.digitcube.hadoop.mapreduce.online.ACUHourMapper;
import net.digitcube.hadoop.mapreduce.online.ACUHourReducer;
import net.digitcube.hadoop.mapreduce.online.PCUHourForAppidMapper;
import net.digitcube.hadoop.mapreduce.online.PCUHourForAppidReducer;
import net.digitcube.hadoop.mapreduce.online.PCUHourMapper;
import net.digitcube.hadoop.mapreduce.online.PCUHourReducer;
import net.digitcube.hadoop.mapreduce.online.UserLoginDayMapper;
import net.digitcube.hadoop.mapreduce.online.UserLoginDayReducer;
import net.digitcube.hadoop.mapreduce.online.UserLoginHourMapper;
import net.digitcube.hadoop.mapreduce.online.UserLoginHourReducer;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayForAppMapper;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayForAppReducer;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayMapper;
import net.digitcube.hadoop.mapreduce.payment.PaymentDayReducer;
import net.digitcube.hadoop.mapreduce.userroll.UserFlowMapper;
import net.digitcube.hadoop.mapreduce.userroll.UserFlowReducer;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingDayMapper;
import net.digitcube.hadoop.mapreduce.userroll.UserInfoRollingDayReducer;
import net.digitcube.hadoop.mapreduce.userroll.UserLostFunnelMapper;
import net.digitcube.hadoop.mapreduce.userroll.UserLostFunnelReducer;
import net.digitcube.hadoop.tmp.LostPlayerGuankaLayoutMapper;
import net.digitcube.hadoop.tmp.LostPlayerGuankaLayoutReducer;
import net.digitcube.hadoop.tmp.LostPlayerLayoutMapper;
import net.digitcube.hadoop.tmp.LostPlayerLayoutReducer;
import net.digitcube.hadoop.tmp.OnlineDetailMapper;
import net.digitcube.hadoop.tmp.OnlineDetailReducer;
import net.digitcube.hadoop.tmp.TmpLevelLayoutMapper;
import net.digitcube.hadoop.tmp.TmpLevelLayoutReducer;
import net.digitcube.hadoop.tmp.UserInfoRollingFilterMapper;
import net.digitcube.hadoop.tmp.datamining.GradeItemMapper;
import net.digitcube.hadoop.tmp.datamining.H5NewAddMonthMapper;
import net.digitcube.hadoop.tmp.datamining.H5NewAddMonthReducer;
import net.digitcube.hadoop.tmp.datamining.IMEIOnlineMapper;
import net.digitcube.hadoop.tmp.datamining.IMEIOnlineReducer;
import net.digitcube.hadoop.tmp.datamining.NewAddPlayerDetailMapper;
import net.digitcube.hadoop.tmp.datamining.TopKBrandCombiner;
import net.digitcube.hadoop.tmp.datamining.TopKBrandMapper;
import net.digitcube.hadoop.tmp.datamining.TopKBrandReducer;
import net.digitcube.hadoop.tmp.datamining.UIDIncrementMapper;
import net.digitcube.hadoop.tmp.datamining.UIDIncrementReducer;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SimpleDriver {

	public static void main(String[] args) throws Exception {
		//executeOnlineDetailJob(args[0], args[1], args[2]);
		//executeL7GuanKaLayoutJob(args[0], args[1]);
		//executeIMEIOnlineJob(args[0], args[1]);
		//executeLostPlayerLayoutJob(args[0], args[1]);
		//executeLevelLayoutJob(args[0], args[1]);
		executeRollingFilterJob(args[0], args[1], args[2]);
		//executeH5NewAddMonthJob(args[0],args[1],args[2]);
		//executeUIDIncrementJob(args[0],args[1],args[2]);
		//executeTopKBrandJob(args[0],args[1],args[2]);
		//excuteNewAddDetailJob(args[0],args[1]);
		//executeAdLabelInHbaseJob(args[0]);
		// excuteBigFields(args[0], args[1]);
		// excuteUidCounter(args[0], args[1]);
		// executeUserInfoRollingInHbaseJob(args[0]);
		// executeDeviceInfoInHbaseJob(args[0]);
		/*
		 * args = new String[]{"actRegSep","/user/rickpan/actRegTest/input", "/user/rickpan/actRegTest/ouput_43"};
		 * String actRegSepJob = "actRegSep"; String newPalyerLayJob = "newPalyerLay"; if (args.length < 3) { LOG.error(
		 * "Please input the 3 given param in the given order: jobName input_path output_path" ); LOG.error("jobName : "
		 * + actRegSepJob + " for active and register player seperator job."); LOG.error("jobName : " + newPalyerLayJob
		 * + " for new player layout statics job."); System.exit(-1); }
		 * 
		 * if (actRegSepJob.equals(args[0])) { //excuteActRegSepJob(args[1], args[2]); excuteActRegSepJobNew(args[1],
		 * args[2]); } else if (newPalyerLayJob.equals(args[0])) { excuteNewPlayerLayoutJob(args[1], args[2]); } else if
		 * ("excuteUserLoginHourJob".equals(args[0])) { excuteUserLoginHourJob(args[1], args[2]); } else if
		 * ("excuteUserLoginDayJob".equals(args[0])) { excuteUserLoginDayJob(args[1], args[2]); } else if
		 * ("excuteACUHourJob".equals(args[0])) { excuteACUHourJob(args[1], args[2]); } else if
		 * ("excutePCUHourJob".equals(args[0])) { excutePCUHourJob(args[1], args[2]); } else if
		 * ("excuteACUHourForAppidJob".equals(args[0])) { excuteACUHourForAppidJob(args[1], args[2]); } else if
		 * ("excutePCUHourForAppidJob".equals(args[0])) { excutePCUHourForAppidJob(args[1], args[2]); } else if
		 * ("excutePaymentDayJob".equals(args[0])) { excutePaymentDayJob(args[1], args[2]); } else if
		 * ("excutePaymentDayForAppJob".equals(args[0])) { excutePaymentDayForAppJob(args[1], args[2]); } else if
		 * ("excuteUserInfoRollingDayJob".equals(args[0])) { excuteUserInfoRollingDayJob(args[1], args[2]); } else if
		 * ("excuteUserFlowJob".equals(args[0])) { excuteUserFlowJob(args[1], args[2]); } else if
		 * ("excuteUserLostFunnelJob".equals(args[0])) { excuteUserLostFunnelJob(args[1], args[2]); } else if
		 * ("excuteUserHabitsJob".equals(args[0])) { excuteUserHabitsJob(args[1], args[2]); }
		 */

	}

	private static boolean onlineDayCheck(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "onlineDayCheck");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(OnlineDayCheckMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setNumReduceTasks(1);
		job.setReducerClass(OnlineDayCheckReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		// FileInputFormat.addInputPath(job, in);
		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;

	}

	private static boolean excuteBigFields(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "BigFields");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(PersonMapper.class);
		job.setMapOutputKeyClass(BigFieldsBaseModel.class);
		job.setMapOutputValueClass(BigFieldsBaseModel.class);

		// reducer
		job.setNumReduceTasks(1);
		job.setReducerClass(PersonReducer.class);
		job.setOutputKeyClass(BigFieldsBaseModel.class);
		job.setOutputValueClass(BigFieldsBaseModel.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		// FileInputFormat.addInputPath(job, in);
		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;

	}

	private static boolean excuteUidCounter(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "UidCounter");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UIDCounterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// combiner
		job.setCombinerClass(UIDCounterReducer.class);

		// reducer
		job.setNumReduceTasks(12);
		job.setReducerClass(UIDCounterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		// FileInputFormat.addInputPath(job, in);
		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;

	}

	public static boolean excuteTmpJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteTmpJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(TmpRollingMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setNumReduceTasks(0);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteActRegSepJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteActRegSepJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(ActRegSeparateMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setNumReduceTasks(0);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteActRegSepJobNew(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapred.job.name", "actRegSepJob");
		conf.set("mapred.jar", "d:/digitcubemr.jar");
		conf.set("mapred.input.dir", inputPath);
		conf.set("mapred.output.dir", outputPath);

		conf.set("mapreduce.map.class", "net.digitcube.hadoop.mapreduce.ActRegPlayerSeperatorMapper");
		conf.set("mapred.mapoutput.key.class", "net.digitcube.hadoop.common.OutFieldsBaseModel");
		conf.set("mapred.mapoutput.value.class", "org.apache.hadoop.io.NullWritable");
		conf.set("mapred.reduce.tasks", "0");
		conf.set("mapreduce.inputformat.class", "org.apache.hadoop.mapreduce.lib.input.TextInputFormat");
		conf.set("mapreduce.outputformat.class", "net.digitcube.hadoop.common.SuffixMultipleOutputFormat");
		// conf.set("mapreduce.outputformat.class","org.apache.hadoop.mapreduce.lib.input.TextOutputFormat");

		conf.set("mapred.mapper.new-api", "true");

		JobConf jobConf = new JobConf(conf);
		JobClient jobClient = new JobClient(jobConf);
		jobClient.submitJob(jobConf);
		System.out.println("submit success...");
		/*
		 * Job job = new Job(conf, "excuteActRegSepJob"); job.setJarByClass(SimpleDriver.class);
		 * 
		 * // mapper job.setMapperClass(ActRegPlayerSeperatorMapper.class);
		 * job.setMapOutputKeyClass(OutFieldsBaseModel.class); job.setMapOutputValueClass(NullWritable.class);
		 * 
		 * // reducer job.setNumReduceTasks(0);
		 * 
		 * // input/output format job.setInputFormatClass(TextInputFormat.class);
		 * job.setOutputFormatClass(SuffixMultipleOutputFormat.class);
		 * 
		 * Path in = new Path(inputPath); Path out = new Path(outputPath); FileInputFormat.addInputPath(job, in);
		 * FileOutputFormat.setOutputPath(job, out);
		 * 
		 * boolean jobExecuteResult = job.waitForCompletion(true);
		 */

		return true;
	}

	// public static boolean excuteNewPlayerLayoutJob(String inputPath,
	// String outputPath) throws Exception {
	//
	// Configuration conf = new Configuration();
	// Job job = new Job(conf, "excuteNewPlayerLayoutJob");
	// job.setJarByClass(SimpleDriver.class);
	//
	// // mapper
	// job.setMapperClass(NewPlayerLayoutMapper.class);
	// job.setMapOutputKeyClass(OutFieldsBaseModel.class);
	// job.setMapOutputValueClass(IntWritable.class);
	//
	// // reducer
	// job.setReducerClass(NewPlayerLayoutReducer.class);
	// job.setOutputKeyClass(OutFieldsBaseModel.class);
	// job.setOutputValueClass(NullWritable.class);
	// job.setNumReduceTasks(1);
	//
	// // input/output format
	// job.setInputFormatClass(TextInputFormat.class);
	// job.setOutputFormatClass(TextOutputFormat.class);
	//
	// Path in = new Path(inputPath);
	// Path out = new Path(outputPath);
	// FileInputFormat.addInputPath(job, in);
	// FileOutputFormat.setOutputPath(job, out);
	//
	// boolean jobExecuteResult = job.waitForCompletion(true);
	//
	// return jobExecuteResult;
	// }

	public static boolean excuteUserLoginHourJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteUserLoginHourJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UserLoginHourMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setReducerClass(UserLoginHourReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(OutFieldsBaseModel.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat1.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteUserLoginDayJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteUserLoginDayJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UserLoginDayMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setReducerClass(UserLoginDayReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(OutFieldsBaseModel.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteACUHourJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteACUHourJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(ACUHourMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(ACUHourReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(IntWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat2.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excutePCUHourJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excutePCUHourJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(PCUHourMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(PCUHourReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(IntWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat2.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteACUHourForAppidJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteACUHourForAppidJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(ACUHourForAppidMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(ACUHourForAppidReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(FloatWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat3.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excutePCUHourForAppidJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excutePCUHourForAppidJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(PCUHourForAppidMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setReducerClass(PCUHourForAppidReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(OutFieldsBaseModel.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat1.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excutePaymentDayJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excutePaymentDayJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(PaymentDayMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setReducerClass(PaymentDayReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(NullWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excutePaymentDayForAppJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excutePaymentDayForAppJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(PaymentDayForAppMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setReducerClass(PaymentDayForAppReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(OutFieldsBaseModel.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat1.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteUserInfoRollingDayJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Calendar calendar = Calendar.getInstance();
		// calendar.add(Calendar.DAY_OF_MONTH, 7);
		conf.set("job.schedule.time", StringUtil.date2yyyy_MM_ddHHmmss(new Date(calendar.getTimeInMillis())));
		Job job = new Job(conf, "excuteUserInfoRollingDayJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UserInfoRollingDayMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setReducerClass(UserInfoRollingDayReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(NullWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		// Path in = new Path(inputPath);
		System.out.println("inputPath=" + inputPath);
		System.out.println("outputPath=" + outputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteUserFlowJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteUserFlowJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UserFlowMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(UserFlowReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(IntWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat2.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteUserLostFunnelJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteUserLostFunnelJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UserLostFunnelMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(UserLostFunnelReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(IntWritable.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat2.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	public static boolean excuteUserHabitsJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "excuteUserHabitsJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UserHabitsMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setReducerClass(UserHabitsReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(OutFieldsBaseModel.class);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat2.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	private static void executeTopKJob(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "executeTopKJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);
	}

	private static void executeUserInfoRollingInHbaseJob(String inputPath) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "dcnamenode1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "dcnamenode1:60000");
		Job job = new Job(conf, "executeUserInfoRollingInHbaseJob");
		job.setJarByClass(SimpleDriver.class);
		// mapper
		job.setMapperClass(UserInfoRollingInHbaseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// .initTableReduceJob("",
		// UserInfoRollingInHbaseReducer.class, job);
		// TableMapReduceUtil.i
		TableMapReduceUtil.initTableReducerJob("users", UserInfoRollingInHbaseReducer.class, job);

		job.setNumReduceTasks(1);
		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(SuffixMultipleOutputFormat.class);
		Path in = new Path(inputPath);
		FileInputFormat.addInputPath(job, in);
		job.waitForCompletion(true);
	}

	private static void executeAdLabelInHbaseJob(String inputPath) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "dcnamenode1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "dcnamenode1:60000");
		conf.set("hbase.tablename.ad_label", "dc_ad_label");
		Job job = new Job(conf, "executeAdLabelInHbaseJob");
		job.setJarByClass(SimpleDriver.class);
		// mapper
		job.setMapperClass(AdLabelInHbaseMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);
		// .initTableReduceJob("",
		// UserInfoRollingInHbaseReducer.class, job);
		// TableMapReduceUtil.i
		// TableMapReduceUtil.initTableReducerJob("users",
		// UserInfoRollingInHbaseReducer.class, job);

		job.setReducerClass(AdLabelInHbaseReducer.class);
		job.setOutputKeyClass(org.apache.hadoop.hbase.io.ImmutableBytesWritable.class);
		job.setOutputValueClass(org.apache.hadoop.hbase.client.Put.class);
		job.setNumReduceTasks(4);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat.class);
		Path in = new Path(inputPath);
		FileInputFormat.addInputPath(job, in);
		job.waitForCompletion(true);
	}

	public static boolean excuteNewAddDetailJob(String inputPath, String outputPath) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "NewAddPlayerDetail");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(NewAddPlayerDetailMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setNumReduceTasks(0);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean jobExecuteResult = job.waitForCompletion(true);

		return jobExecuteResult;
	}

	private static void executeUIDIncrementJob(String inputPath, String outputPath, String type) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "UIDIncrementJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UIDIncrementMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		if ("Y".equals(type)) {
			job.setCombinerClass(UIDIncrementReducer.class);
		}
		// reducer
		job.setReducerClass(UIDIncrementReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);
	}

	private static void executeTopKBrandJob(String inputPath, String outputPath, String type) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TopKBrandJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(TopKBrandMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		if ("Y".equals(type)) {
			job.setCombinerClass(TopKBrandCombiner.class);
		}
		// reducer
		job.setReducerClass(TopKBrandReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);
	}

	private static void executeH5NewAddMonthJob(String inputPath, String outputPath, String type) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "H5NewAddMonth");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(H5NewAddMonthMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		if ("Y".equals(type)) {
			job.setCombinerClass(H5NewAddMonthReducer.class);
		}
		// reducer
		job.setReducerClass(H5NewAddMonthReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);
	}
	
	private static void executeLevelLayoutJob(String inputPath, String outputPath)
			throws Exception {
		if(!inputPath.contains("/data/digitcube/rolling_day/2015/03/30/00/output/")){
			throw new RuntimeException("the input path's date of is wrong...");
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "LevelLayoutJob");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(TmpLevelLayoutMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setReducerClass(TmpLevelLayoutReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		// input/output format
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);
	}
	
	private static void executeLostPlayerLayoutJob(String inputPath, String outputPath) throws Exception {
		if(!inputPath.contains("/data/digitcube/rolling_day/2015/03/31/00/output/")
				&& !inputPath.contains("/data/digitcube/rolling_day/2015/04/01/00/output/")
				&& !inputPath.contains("/data/digitcube/rolling_day/2015/04/02/00/output/")){
			throw new RuntimeException("the input path's date of is wrong...");
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "3DayLostPlayer");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(LostPlayerLayoutMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(LostPlayerLayoutReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		// input/output format
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);
	}

	private static void executeL7GuanKaLayoutJob(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "L7GuanKaLayout");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(LostPlayerGuankaLayoutMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);

		// reducer
		job.setReducerClass(LostPlayerGuankaLayoutReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);

		// input/output format
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		//FileInputFormat.addInputPath(job, in);
		FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, out);

		//job.waitForCompletion(true);
		job.submit();
	}
	

	private static void executeOnlineDetailJob(String date, String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		conf.set("dc.tmp.date", date);
		Job job = new Job(conf, "OnlineDetail");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(OnlineDetailMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// reducer
		job.setReducerClass(OnlineDetailReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);

		// input/output format
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		//FileInputFormat.addInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, out);

		//job.waitForCompletion(true);
		job.submit();
	}
	
	private static void executeRollingFilterJob(String inputPath, String outputPath, String appId) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "RollingFilter");
		job.getConfiguration().set("targetAppId2Filter", appId);
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(UserInfoRollingFilterMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setNumReduceTasks(0);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);
		

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		//job.waitForCompletion(true);
		// job.waitForCompletion(true);
		job.submit();
	}



	private static void executeIMEIOnlineJob(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "IMEIOnline");
		job.setJarByClass(SimpleDriver.class);

		// mapper
		job.setMapperClass(IMEIOnlineMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setReducerClass(IMEIOnlineReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(5);

		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);

		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// job.waitForCompletion(true);
		job.submit();
	}
	
	private static void executeHelloKettyJob(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "HelloKetty");
		job.setJarByClass(SimpleDriver.class);
		
		// mapper
		job.setMapperClass(GradeItemMapper.class);
		job.setMapOutputKeyClass(OutFieldsBaseModel.class);
		job.setMapOutputValueClass(OutFieldsBaseModel.class);
		
		// reducer
		job.setReducerClass(IMEIOnlineReducer.class);
		job.setOutputKeyClass(OutFieldsBaseModel.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(5);
		
		// input/output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SuffixMultipleOutputFormat.class);
		
		Path in = new Path(inputPath);
		Path out = new Path(outputPath);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// job.waitForCompletion(true);
		job.submit();
	}
}
