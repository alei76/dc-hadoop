package net.digitcube.hadoop.common;


public class SuffixMultipleOutputFormat1 extends
		SuffixOutputFormatBase<SuffixBase, OutFieldsBaseModel> {

	@Override
	public String generateFileNameForKeyValue(SuffixBase key,
			OutFieldsBaseModel value, String partition) {

		return "part-r-" + partition + "-" + key.getSuffix();
	}

}
