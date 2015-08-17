package net.digitcube.hadoop.common;

import org.apache.hadoop.io.FloatWritable;

public class SuffixMultipleOutputFormat3 extends
		SuffixOutputFormatBase<SuffixBase, FloatWritable> {

	@Override
	public String generateFileNameForKeyValue(SuffixBase key,
			FloatWritable value, String partition) {

		return "part-r-" + partition + "-" + key.getSuffix();
	}

}
