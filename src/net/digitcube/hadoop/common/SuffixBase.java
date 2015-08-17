package net.digitcube.hadoop.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SuffixBase implements WritableComparable<SuffixBase>{

	public String suffix = "";

	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	@Override
	public int compareTo(SuffixBase arg0) {
		return this.suffix.compareTo(arg0.getSuffix());
		//return CompareToBuilder.reflectionCompare(this, arg0);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(suffix);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		suffix = in.readUTF();
	}
}
