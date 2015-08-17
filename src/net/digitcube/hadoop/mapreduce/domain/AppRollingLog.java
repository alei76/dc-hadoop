package net.digitcube.hadoop.mapreduce.domain;

import java.util.ArrayList;
import java.util.List;

import net.digitcube.hadoop.jce.GZIPUtils;
import net.digitcube.protocol.HexUtil;
import net.digitcube.protocol.JceObjectUtil;

/**
 * 设备上面安装的手机应用的情况
 */
public class AppRollingLog {
	private String appID;
	private String version;
	private String platform;
	private String channel;
	private String gameServer;
	private String uid;
	// 存放上次上报的手机安装的应用列表
	private List<AppDetail> appList;
	// 存放上次上报的手机铃声列表
	private List<AppDetail> ringList;

	public AppRollingLog() {
	};

	public AppRollingLog(String[] args) {
		int i = 0;
		this.appID = args[i++];
		this.version = args[i++];
		this.platform = args[i++];
		this.channel = args[i++];
		this.gameServer = args[i++];
		this.uid = args[i++];
		String appListString = args[i++];
		try {
			this.appList = "0".equals(appListString) ? new ArrayList<AppDetail>() : JceObjectUtil.bytes2List(
					GZIPUtils.decompress(HexUtil.hexStr2Bytes(appListString)), AppDetail.class);
		} catch (Exception e) {
			this.appList = new ArrayList<AppDetail>();
		}

		String ringListString = args[i++];
		try {
			this.ringList = "0".equals(ringListString) ? new ArrayList<AppDetail>() : JceObjectUtil.bytes2List(
					GZIPUtils.decompress(HexUtil.hexStr2Bytes(ringListString)), AppDetail.class);
		} catch (Exception e) {
			this.ringList = new ArrayList<AppDetail>();
		}
	}

	public String getAppID() {
		return appID;
	}

	public void setAppID(String appID) {
		this.appID = appID;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getGameServer() {
		return gameServer;
	}

	public void setGameServer(String gameServer) {
		this.gameServer = gameServer;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public List<AppDetail> getAppList() {
		return appList;
	}

	public void setAppList(List<AppDetail> appList) {
		this.appList = appList;
	}

	public List<AppDetail> getRingList() {
		return ringList;
	}

	public void setRingList(List<AppDetail> ringList) {
		this.ringList = ringList;
	}

	public String[] toStringArray() {
		String appHexString = "0";
		try {
			byte[] data = JceObjectUtil.list2Bytes(this.appList);
			// 进行压缩
			if (data != null) {
				appHexString = HexUtil.bytes2HexStr(GZIPUtils.compress(data));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		String ringHexString = "0";
		try {
			byte[] data = JceObjectUtil.list2Bytes(this.ringList);
			if (data != null)
				ringHexString = HexUtil.bytes2HexStr(GZIPUtils.compress(data));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return new String[] { this.appID, this.version, this.platform, this.channel, this.gameServer, this.uid,
				appHexString, ringHexString };
	}

	public static void main(String[] args) throws Exception {
		AppRollingLog appRollingLog = new AppRollingLog();
		appRollingLog.setAppID("appid");
		appRollingLog.setChannel("channel");
		appRollingLog.setGameServer("gameserver");
		appRollingLog.setPlatform("platform");
		appRollingLog.setUid("uid");
		appRollingLog.setVersion("version");
		List<AppDetail> appList = new ArrayList<AppDetail>();
		AppDetail appDetail = new AppDetail("package", "360", "1.0", 123, 123, 33, 22, "");
		AppDetail appDetail2 = new AppDetail("package2", "xunlei", "1.0", 123, 123, 33, 22, "");
		appList.add(appDetail);
		appList.add(appDetail2);
		appRollingLog.setAppList(appList);
		for (String str : appRollingLog.toStringArray()) {
			System.out.println(str);
		}

		String src = "1F8B08000000000000006D54416C1B451475657BDAB44943216C9520A108953DA076648F9BD4ADAAA28C53A412508075C8A59749766C6FBBBBB3ECAEA9D323A85520142CA10A352A1208A1822A558AA222D4435B959C50259443D5038803921D73E1548E04FEF77AEA2255F63EFBFFF57BFFCDFFFB9D4A655F49A552C788B1A83C7A4EF8B6AA9F71040D6A4AFA4E8319039DB54F3A6B173B97564D7298E6E1CDCAE48F0F38C269F7FD1D87805D2423B221BCC095B4E2B8D256E77C5709DB187C0DA2E95E64A6F334C7ACDFB7EF7304CD7D953C8B95DF736A4A51110451AC42690C172673ED8F56DA576FB53EFE1EBE9803799A3F4A73F4C804B356975FE4E5B14C514BE412096DA1518F1D373248F2D92BBB961EE2D65AF68AE6CC90311556A9A7C278412C51E78C8CE325BAA8FC48B9D218750EBD8E89F15292183FE547B1705D199AE90274C05A7FF83247E85BD85755AA0AD5A3A528961EF5043800B0A667CC0C381867D6E686E2D6E6CF1B9AC3939EC38F683D70C592882207AAF8B131741E1A31A54373F7BB2080AF5C8159BF347683CAFD52BF7F235A25B2CFF63506217A9A0463D69D7917254E68891364F4C9FE2DBA5284D276E25836C04B09C3931096214CBA59DEB9DDE4E59DFFA6B50223CF681355E149963B5C34328838B71C852B37C9AC47333F7204CD3A95D4C5CB16B1F01C5FF6FDEFD3A9A71D0226B0F1D33D8EA0C5F2641085E64A6FAA0578EA8C3D73A5F6CDCF3AD7965B57BE33B3476911C7B6FAE73A47D0A423E479ED3B29AF9FBFCCF9A92040F3856EBD09561E2DBE010FDDC91B9AFA167909A935D8184F84676950775DA4564219D568D46D65648CFC2FDDEBB09965D012D8A3E1DFB63982D69C222F2CFAD4569E5AA0AA5291212CA4EB5228112AC736F64EE39D59BC310FF9DE93FDA075875B0F3A97B5C82479EEB188B0A39E15634F976C2506D2ACBB8ABF9EE608FD6EECEDAF62A8549CACE1A7CBED2F3FDCBAF1F556F3A2999DC47142239B4D8EA0A9C7C9FEC754E0C05EE110225181C2F7BE824CEBD2F5D637D7CDEC0465B48083F88B5B1D36A0F9253286FC48D5EAD4F1837AECC9B8A66C485455DD186E37AF6EAD7CD1B9FD79EBC2B7EDF5CB6616C7C2D8DB770FFCCD11B4CC01B20B656219C54606B1D7A347739BDCFA6765BE7FD227FE7402197ADDD30E81CFCE0F17B66E5D7B6776B68C5486273D88273DD8A5FE0748E0310D2E050000";
		System.out.println(src.length());
		List<AppDetail> list = JceObjectUtil.bytes2List(GZIPUtils.decompress(HexUtil.hexStr2Bytes(src)),
				AppDetail.class);
		System.out.println(list);
	}
}
