package net.digitcube.hadoop.util;

public class FieldValidationUtil {
	
	/*public static boolean validateAppIdLength(String appId) {
		boolean flag = true;
		try {
			if(!StringUtil.isEmpty(appId)){
				String[] id = StringUtil.split(appId, "|");
				if(id.length < 2 
						|| StringUtil.isEmpty(id[0]) 
						|| id[0].length() != 32){
					flag = false;
				}
			}else{				
				flag = false;
			}
		} catch (Throwable t) {
			flag = false;
		}
		return flag;
	}*/

	public static boolean validateAppIdLength(String appId) {
		if(StringUtil.isEmpty(appId)){
			return false;
		}
		
		String pureAppId = appId.split("\\|")[0];
		if(pureAppId.length() < 32){
			return false;
		}
		
		return true;
	}
	
	public static void main(String[] args) {
		String bn = "";
		//boolean a = FieldValidationUtil.validateAppIdLength(bn);
		System.out.println(StringUtil.split(bn, "|").length);
		
		
	}

}
