package test.taskanditem;

import net.digitcube.hadoop.model.UserExtInfoRollingLog;

public class Test {
	


	public static void main(String[] args) {
		String detailInfo = "H4sIAAAAAAAAAONgYGQKSbnaIMXJwMHFxmZoYGBgaCHG9rJx8tPW7ToGTD4VDIxsPE8n7n2xf8LT3oXPO3sEmLiBCs0MQCrF2MBi7ToGIkCFTGz8z1fPe9bV8HTOrqdLpz3t2ijAysbzbH3_s-lLXy5qe9o_TYCfG2qJsSHCEkYHRkxbGDuAQkJPdk9-umvyk737n09ZAVEuwAgywsLA0Ahk_fNZ6140LQEaAXYnx7OuvUDt8YYQRaZARYZIbsSqyBDkYSOEY5ghPkZxNTPM1SZIQQMxDcPHMEPBoYOqFs2D3JI8mjzcAGk_QhiBAQAA";
		UserExtInfoRollingLog log = new UserExtInfoRollingLog("appID", "2",  "channel", "gameServer",  "accountId",  detailInfo);
		System.out.println(log.getDetailMap());
		
		

	}

}
