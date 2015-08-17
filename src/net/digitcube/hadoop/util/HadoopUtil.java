package net.digitcube.hadoop.util;

import java.io.IOException;

import net.digitcube.hadoop.common.Constants;

import org.apache.hadoop.conf.Configuration;

public class HadoopUtil {

	/**
	 * 检查用户是否已指定日志的输出后缀
	 * Constants.KEY_XXX_NATURE_WEEK 和 Constants.KEY_XXX_NATURE_MONTH
	 * 必须有且只能有一个为 true
	 * 如果是自然周任务，则加上自然周后缀，如果是自然月任务，则加上自然月后缀
	 */
	public static void ensureNatureWeekOrMonthIsSet(Configuration conf, String keyNatureWeek, String keyNatureMonth) throws IOException {
		 
		if( (false == conf.getBoolean(keyNatureWeek, false))
		 && (false == conf.getBoolean(keyNatureMonth, false))){
			throwWeekOrMonthNotSetExp(keyNatureWeek, keyNatureMonth);
		}
	}
	
	
	public static void ensurePaymentNatureWeekOrMonthIsSet(Configuration conf) throws IOException {
		 
		if( (false == conf.getBoolean(Constants.KEY_PAYMENT_NATURE_WEEK, false))
		 && (false == conf.getBoolean(Constants.KEY_PAYMENT_NATURE_MONTH, false))){
			
			throwWeekOrMonthNotSetExp("","");
		}
	}
	
	public static void throwWeekOrMonthNotSetExp(String keyNatureWeek, String keyNatureMonth) throws IOException{
		throw new IOException( keyNatureWeek + "=true or " + keyNatureMonth+ "=true is not set." 
							 + keyNatureWeek+" means this is a nature week job,"
							 + keyNatureMonth+" means this is a nature month payment job,"
							 + " user must set one of them for the job...");
	}
}
