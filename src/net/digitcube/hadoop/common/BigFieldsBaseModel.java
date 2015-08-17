package net.digitcube.hadoop.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

public class BigFieldsBaseModel extends SuffixBase implements
		WritableComparable<SuffixBase> {

	String[] outFields;

	public String[] getOutFields() {
		return outFields;
	}

	public void setOutFields(String[] outFields) {
		this.outFields = outFields;
	}

	public BigFieldsBaseModel() {
	}

	public BigFieldsBaseModel(String[] outFields) {
		setOutFields(outFields);
	}

	@Override
	public int compareTo(SuffixBase arg0) {
		return CompareToBuilder.reflectionCompare(this, arg0);
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj); 
	}
	
	@Override
	public int hashCode(){
		return HashCodeBuilder.reflectionHashCode(this); 
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// write suffix
		super.write(out);

		// write out fields
		if (null == outFields) {
			outFields = new String[0];
		}
		out.writeInt(outFields.length);
		for (int i = 0; i < outFields.length; i++) {
			//out.writeUTF(outFields[i]);
			//writeUTF 最大只能写 65536 个字节，不满足需求
			byte[] b = outFields[i].getBytes("utf-8");
			out.writeInt(b.length);
			out.write(b);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// read suffix
		super.readFields(in);

		// read out fields
		outFields = new String[in.readInt()];
		for (int i = 0; i < outFields.length; i++) {
			int bytes = in.readInt();
			byte[] b = new byte[bytes];
			in.readFully(b);
			outFields[i] = new String(b, "utf-8");
		}
	}

	@Override
	public String toString() {

		StringBuffer strBuf = new StringBuffer();
		for (int i = 0; i < outFields.length; i++) {
			strBuf.append(MRConstants.SEPERATOR_OUT).append(outFields[i]);
		}

		return strBuf.substring(MRConstants.SEPERATOR_OUT.length(),
				strBuf.length());
	}
	
	public void reset(){
		super.setSuffix("");
		this.setOutFields(null);
	}
}
