package net.digitcube.hadoop.util;

import java.util.HashSet;
import java.util.Set;

import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;

/**
 * 该类主要用于修正战国之王部分玩家区服乱码的情况
 * 2013.11.07 以修正完成，该类将不会再使用
 */
public class GRCrashedCodeConvertor {

	private static final String appId = "8EFA6FD1DC7E2A283EF56EF6BEADC35B";
	private static Set<String> gRCodeCrashedAccSet = new HashSet<String>();
	//战国之王下面 48 个帐号区服是乱码，将统一替换
	static{
		gRCodeCrashedAccSet.add("1382081532820");
		gRCodeCrashedAccSet.add("1382083494722");
		gRCodeCrashedAccSet.add("1377052813825");
		gRCodeCrashedAccSet.add("1382083260838");
		gRCodeCrashedAccSet.add("1382084097445");
		gRCodeCrashedAccSet.add("1382083292105");
		gRCodeCrashedAccSet.add("1382083612967");
		gRCodeCrashedAccSet.add("1382083591510");
		gRCodeCrashedAccSet.add("1374545637358");
		gRCodeCrashedAccSet.add("1382083202684");
		gRCodeCrashedAccSet.add("1382080848711");
		gRCodeCrashedAccSet.add("1382080586287");
		gRCodeCrashedAccSet.add("1382080392052");
		gRCodeCrashedAccSet.add("1382080994983");
		gRCodeCrashedAccSet.add("1382081280468");
		gRCodeCrashedAccSet.add("1372913083909");
		gRCodeCrashedAccSet.add("1382083458246");
		gRCodeCrashedAccSet.add("1382083429996");
		gRCodeCrashedAccSet.add("1382082486284");
		gRCodeCrashedAccSet.add("1382083207035");
		gRCodeCrashedAccSet.add("1382083475879");
		gRCodeCrashedAccSet.add("1382083653090");
		gRCodeCrashedAccSet.add("1382083545017");
		gRCodeCrashedAccSet.add("1382081457066");
		gRCodeCrashedAccSet.add("1382081608054");
		gRCodeCrashedAccSet.add("1382083890272");
		gRCodeCrashedAccSet.add("1382083619139");
		gRCodeCrashedAccSet.add("1382083065963");
		gRCodeCrashedAccSet.add("1382083093052");
		gRCodeCrashedAccSet.add("1382083050656");
		gRCodeCrashedAccSet.add("1382083686737");
		gRCodeCrashedAccSet.add("1382083330584");
		gRCodeCrashedAccSet.add("1382080963549");
		gRCodeCrashedAccSet.add("1382084107397");
		gRCodeCrashedAccSet.add("1382081077797");
		gRCodeCrashedAccSet.add("1382081113238");
		gRCodeCrashedAccSet.add("1382083585905");
		gRCodeCrashedAccSet.add("1382083321308");
		gRCodeCrashedAccSet.add("1382083379098");
		gRCodeCrashedAccSet.add("1382080757955");
		gRCodeCrashedAccSet.add("1382080855231");
		gRCodeCrashedAccSet.add("1380111543214");
		gRCodeCrashedAccSet.add("1376536183895");
		gRCodeCrashedAccSet.add("1382081453807");
		gRCodeCrashedAccSet.add("1382083399298");
		gRCodeCrashedAccSet.add("1382083413024");
		gRCodeCrashedAccSet.add("1382083579913");
		gRCodeCrashedAccSet.add("1382081114335");
		
		//new add 1023
		gRCodeCrashedAccSet.add("1374117359115");
		gRCodeCrashedAccSet.add("1380101510773");
		gRCodeCrashedAccSet.add("1380109115354");
		gRCodeCrashedAccSet.add("1380111305506");
		gRCodeCrashedAccSet.add("1382083292736");
		gRCodeCrashedAccSet.add("1376536527372");
		gRCodeCrashedAccSet.add("1379474403238");
		gRCodeCrashedAccSet.add("1380099030956");
		gRCodeCrashedAccSet.add("1382083274798");
		gRCodeCrashedAccSet.add("1372919658798");
		gRCodeCrashedAccSet.add("1377062878917");
		gRCodeCrashedAccSet.add("1382083074005");
		gRCodeCrashedAccSet.add("1382083308328");
		gRCodeCrashedAccSet.add("1382083526875");
		gRCodeCrashedAccSet.add("1377059574693");
		gRCodeCrashedAccSet.add("1380101766744");
		gRCodeCrashedAccSet.add("1382083449436");
	}
	
	public static void checkAndSetGR(UserInfoRollingLog userInfoRollingLog){
		//必须是战国之王以及指定区服乱码的帐号
		/*if(userInfoRollingLog.getAppID().contains(appId) && gRCodeCrashedAccSet.contains(userInfoRollingLog.getAccountID())){
			String gameRegion = userInfoRollingLog.getPlayerDayInfo().getGameRegion();
			if(null == gameRegion){
				return;
			}
			if(gameRegion.startsWith("1F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("1F乱世风云");
			}else if(gameRegion.startsWith("2F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("2F征战四方");
			}else if(gameRegion.startsWith("3F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("3F剑啸群雄");
			}else if(gameRegion.startsWith("4F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("4F叱咤风云");
			}else if(gameRegion.startsWith("5F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("5F南征北战");
			}else if(gameRegion.startsWith("6F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("6F群雄逐鹿");
			}else if(gameRegion.startsWith("7F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("7F问鼎中原");
			}
		}*/
		
		if(userInfoRollingLog.getAppID().contains(appId)){
			String gameRegion = userInfoRollingLog.getPlayerDayInfo().getGameRegion();
			if(null == gameRegion){
				return;
			}
			if(gameRegion.startsWith("1F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("1F乱世风云");
			}else if(gameRegion.startsWith("2F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("2F征战四方");
			}else if(gameRegion.startsWith("3F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("3F剑啸群雄");
			}else if(gameRegion.startsWith("4F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("4F叱咤风云");
			}else if(gameRegion.startsWith("5F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("5F南征北战");
			}else if(gameRegion.startsWith("6F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("6F群雄逐鹿");
			}else if(gameRegion.startsWith("7F")){
				userInfoRollingLog.getPlayerDayInfo().setGameRegion("7F问鼎中原");
			}
		}
	}
	
	public static void main(String[] args){
		System.out.println(gRCodeCrashedAccSet.size());
	}
}
