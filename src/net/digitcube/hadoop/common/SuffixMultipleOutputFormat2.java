package net.digitcube.hadoop.common;

import org.apache.hadoop.io.IntWritable;

public class SuffixMultipleOutputFormat2 extends
		SuffixOutputFormatBase<SuffixBase, IntWritable> {

	@Override
	public String generateFileNameForKeyValue(SuffixBase key,
			IntWritable value, String partition) {

		return "part-r-" + partition + "-" + key.getSuffix();
	}

}
