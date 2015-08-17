package net.digitcube.hadoop.common;

import org.apache.hadoop.io.Writable;

public class SuffixMultipleOutputFormat extends SuffixOutputFormatBase<SuffixBase, Writable> {

	@Override
	public String generateFileNameForKeyValue(SuffixBase key, Writable value, String partition) {
		
		return "part-r-" + partition + "-" + key.getSuffix();
	}

}
