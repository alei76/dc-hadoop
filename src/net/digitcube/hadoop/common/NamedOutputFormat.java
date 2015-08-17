package net.digitcube.hadoop.common;

import org.apache.hadoop.io.NullWritable;

public class NamedOutputFormat extends SuffixOutputFormatBase<SuffixBase, NullWritable> {

	@Override
	public String generateFileNameForKeyValue(SuffixBase key, NullWritable value, String partition) {
		
		return "part-r-" + partition + "-" + key.getSuffix();
	}

}
