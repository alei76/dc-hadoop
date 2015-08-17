package net.digitcube.hadoop.common;

public final class Gender implements java.io.Serializable {
	private static Gender[] __values = new Gender[3];
	private int __value;
	private String __T = new String();

	public static final int _UNKNOWN = 0;
	public static final Gender UNKNOWN = new Gender(0, _UNKNOWN, "未知");
	public static final int _MALE = 1;
	public static final Gender MALE = new Gender(1, _MALE, "男");
	public static final int _FEMALE = 2;
	public static final Gender FEMALE = new Gender(2, _FEMALE, "女");

	public static Gender convert(int val) {
		for (int __i = 0; __i < __values.length; ++__i) {
			if (__values[__i].value() == val) {
				return __values[__i];
			}
		}
		assert false;
		return null;
	}

	public static Gender convert(String val) {
		for (int __i = 0; __i < __values.length; ++__i) {
			if (__values[__i].toString().equals(val)) {
				return __values[__i];
			}
		}
		assert false;
		return null;
	}

	public int value() {
		return __value;
	}

	public String toString() {
		return __T;
	}

	private Gender(int index, int val, String s) {
		__T = s;
		__value = val;
		__values[index] = this;
	}

}
