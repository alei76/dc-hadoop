package net.digitcube.hadoop.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;

public class KVFieldsBaseModel extends SuffixBase implements WritableComparable<SuffixBase>{

	String[] keyFields;
	String[] valueFields;
	public String[] getKeyFields() {
		return keyFields;
	}
	public void setKeyFields(String[] keyFields) {
		this.keyFields = keyFields;
	}
	public String[] getValueFields() {
		return valueFields;
	}
	public void setValueFields(String[] valueFields) {
		this.valueFields = valueFields;
	}
	
	@Override
	public int compareTo(SuffixBase arg0) {
		return CompareToBuilder.reflectionCompare(this, arg0);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		//write key fields
		if (null == keyFields) {
			keyFields = new String[0];
		}
		out.writeInt(keyFields.length);
		for(int i = 0; i < keyFields.length; i++){
			out.writeUTF(keyFields[i]);
		}
		
		//write key fields
		if (null == valueFields) {
			valueFields = new String[0];
		}
		out.writeInt(valueFields.length);
		for(int i = 0; i < valueFields.length; i++){
			out.writeUTF(valueFields[i]);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		//read key fields
		keyFields = new String[in.readInt()];
		for(int i = 0; i < keyFields.length; i++){
			keyFields[i] = in.readUTF();
		}
		
		//read key fields
		valueFields = new String[in.readInt()];
		for(int i = 0; i < valueFields.length; i++){
			valueFields[i] = in.readUTF();
		}
	}
	
	@Override
	public String toString() {
		
		StringBuffer strBuf = new StringBuffer();
		for(int i = 0; i < keyFields.length; i++){
			strBuf.append(MRConstants.SEPERATOR_OUT).append(keyFields[i]);
		}
		for(int i = 0; i < valueFields.length; i++){
			strBuf.append(MRConstants.SEPERATOR_OUT).append(valueFields[i]);
		}
		
		return strBuf.substring(MRConstants.SEPERATOR_OUT.length(), strBuf.length());
	}
}
