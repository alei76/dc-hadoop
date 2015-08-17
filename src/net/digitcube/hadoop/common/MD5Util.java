package net.digitcube.hadoop.common;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {

	private static MessageDigest md = null;
	private static StringBuffer buf = new StringBuffer();
	static{
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	public static synchronized String getMD5Str(String sourceStr) throws UnsupportedEncodingException{
		buf.delete(0, buf.length());
		md.reset();
		md.update(sourceStr.getBytes("UTF-8"));
		byte b[] = md.digest();
		
		int byteVal = 0;
        for (int offset = 0; offset < b.length; offset++) {
        	byteVal = b[offset];
            if (byteVal < 0){
            	byteVal += 256;
            }
            if (byteVal < 16){
            	buf.append("0");
            }
            buf.append(Integer.toHexString(byteVal));
        }
        
        return buf.toString();
	}
}
