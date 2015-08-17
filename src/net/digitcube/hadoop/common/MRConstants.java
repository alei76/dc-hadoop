package net.digitcube.hadoop.common;

public interface MRConstants {

	public static final String SEPERATOR_IN = "\t";
	public static final String SEPERATOR_OUT = "\t";
	
	public static final String SEPERATOR_SCENE_ARRAY = ";";
	public static final String SEPERATOR_SCENE_POINT = "-";
	public static final String SEPERATOR_SCENE_RESOLUTION = "x";
	
	public static final String INVALID_PLACE_HOLDER_NUM = "0";
	public static final String INVALID_PLACE_HOLDER_CHAR = "-";
	public static final String INVALID_PLACE_INSTEAD_CHAR = "000000000";
	
	//当不分区服统计的时候，用  ALL_GAMESERVER 标识全服统计
	public static final String ALL_GAMESERVER = "_ALL_GS";
	
	//
	public static final String ALL_VERSION = "_ALL_VR";
	public static final String ALL_CHANNEL = "_ALL_CH";
	
	//分隔符
	public static final String DC_SEPARATOR = "_DC_SEP_";
	
	//平台
	public static final String PLATFORM_iOS_STR = "1";
	public static final String PLATFORM_ANDROID_STR = "2";
	public static final String PLATFORM_WINDOWS_STR = "3";
	public static final int PLATFORM_iOS_INT = 1;
	public static final int PLATFORM_ANDROID_INT = 2;
	public static final int PLATFORM_WINDOWS_INT = 3;
}
