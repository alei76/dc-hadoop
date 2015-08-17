package net.digitcube.hadoop.util;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

public class JobGenerator {

	private static final Log LOGGER = LogFactory.getLog(JobGenerator.class);

	private Job job;
	private FileSystem fs;

	public JobGenerator(final String name) throws IOException {
		try {
			UserGroupInformation.createRemoteUser("hadoop").doAs(
					new PrivilegedExceptionAction<Void>() {
						@Override
						public Void run() throws IOException {
							JobConf conf = new JobConf();
							conf.setJobPriority(JobPriority.VERY_HIGH);
							conf.setMapOutputCompressorClass(SnappyCodec.class);
							conf.setCompressMapOutput(true);

							job = new Job(conf);
							job.setJarByClass(this.getClass());
							job.setJobName(name);

							// set num of reducer
							job.setNumReduceTasks(10);

							// disable speculate
							job.setMapSpeculativeExecution(false);
							job.setReduceSpeculativeExecution(false);

							fs = FileSystem.get(job.getConfiguration());
							return null;
						}
					});
		} catch (Exception e) {
			throw new IOException("unexpected exception when creating jobs", e);
		}
	}

	public JobGenerator input(Path[] paths) throws IOException {
		FileInputFormat.setInputPaths(job, paths);
		return this;
	}

	public JobGenerator output(Path output) throws IOException {
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileOutputFormat.setOutputPath(job, output);
		return this;
	}

	public JobGenerator inputFormat(Class<? extends InputFormat<?, ?>> input) {
		this.job.setInputFormatClass(input);
		return this;
	}

	public JobGenerator outputFormat(Class<? extends OutputFormat<?, ?>> output) {
		this.job.setOutputFormatClass(output);
		return this;
	}

	public JobGenerator mapType(Class<? extends Mapper<?, ?, ?, ?>> mapper) {
		job.setMapperClass(mapper);
		return this;
	}

	public JobGenerator mapKeyType(Class<? extends WritableComparable<?>> key) {
		this.job.setMapOutputKeyClass(key);
		return this;
	}

	public JobGenerator mapValueType(Class<? extends Writable> value) {
		this.job.setMapOutputValueClass(value);
		return this;
	}

	public JobGenerator reduceType(Class<? extends Reducer<?, ?, ?, ?>> reducer) {
		job.setReducerClass(reducer);
		return this;
	}

	public JobGenerator outKeyType(Class<? extends WritableComparable<?>> key) {
		this.job.setOutputKeyClass(key);
		return this;
	}

	public JobGenerator outValueType(Class<? extends Writable> value) {
		this.job.setOutputValueClass(value);
		return this;
	}

	public void execute() {
		UserGroupInformation.createRemoteUser("hadoop").doAs(
				new PrivilegedAction<Void>() {
					@Override
					public Void run() {
						try {
							job.waitForCompletion(true);
						} catch (Exception e) {
							LOGGER.error(
									"unexpected excetpion when waiting for job completion",
									e);
						}
						return null;
					}
				});
	}
}
