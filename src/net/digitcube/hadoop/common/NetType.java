package net.digitcube.hadoop.common;

public final class NetType implements java.io.Serializable {
	private static NetType[] __values = new NetType[14];
	private int __value;
	private String __T = new String();

	public static final int _GPRS = 0;
	public static final NetType GPRS = new NetType(0, _GPRS, "GPRS");
	public static final int _G3 = 1;
	public static final NetType G3 = new NetType(1, _G3, "G3");
	public static final int _WIFI = 2;
	public static final NetType WIFI = new NetType(2, _WIFI, "WIFI");
	public static final int _OTHER = 3;
	public static final NetType OTHER = new NetType(3, _OTHER, "OTHER");
	public static final int _EDGE = 4;
	public static final NetType EDGE = new NetType(4, _EDGE, "EDGE");
	public static final int _UMTS = 5;
	public static final NetType UMTS = new NetType(5, _UMTS, "UMTS");
	public static final int _CDMA = 6;
	public static final NetType CDMA = new NetType(6, _CDMA, "CDMA");
	public static final int _EVDO_0 = 7;
	public static final NetType EVDO_0 = new NetType(7, _EVDO_0, "EVDO_0");
	public static final int _EVDO_A = 8;
	public static final NetType EVDO_A = new NetType(8, _EVDO_A, "EVDO_A");
	public static final int _X1RTT = 9;
	public static final NetType X1RTT = new NetType(9, _X1RTT, "X1RTT");
	public static final int _HSDPA = 10;
	public static final NetType HSDPA = new NetType(10, _HSDPA, "HSDPA");
	public static final int _HSUPA = 11;
	public static final NetType HSUPA = new NetType(11, _HSUPA, "HSUPA");
	public static final int _HSPA = 12;
	public static final NetType HSPA = new NetType(12, _HSPA, "HSPA");
	public static final int _IDEN = 13;
	public static final NetType IDEN = new NetType(13, _IDEN, "IDEN");

	public static NetType convert(int val) {
		for (int __i = 0; __i < __values.length; ++__i) {
			if (__values[__i].value() == val) {
				return __values[__i];
			}
		}
		assert false;
		return null;
	}

	public static NetType convert(String val) {
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

	private NetType(int index, int val, String s) {
		__T = s;
		__value = val;
		__values[index] = this;
	}

}
