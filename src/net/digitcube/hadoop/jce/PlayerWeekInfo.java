// **********************************************************************
// This file was generated by a TAF parser!
// TAF version 3.0.0.25 by WSRD Tencent.
// Generated from `playerinfo.jce'
// **********************************************************************

package net.digitcube.hadoop.jce;

import net.digitcube.protocol.JceDisplayer;
import net.digitcube.protocol.JceInputStream;
import net.digitcube.protocol.JceOutputStream;
import net.digitcube.protocol.JceStruct;
import net.digitcube.protocol.JceUtil;

public final class PlayerWeekInfo extends JceStruct implements
		java.lang.Cloneable {
	public String className() {
		return "PlayerInfo.PlayerWeekInfo";
	}

	public String fullClassName() {
		return "net.digitcube.hadoop.jce.PlayerInfo.PlayerWeekInfo";
	}

	public int firstLoginWeekDate = 0;

	public int lastLoginWeekDate = 0;

	public int firstPayWeekDate = 0;

	public int lastPayWeekDate = 0;

	public String channel = "";

	public String gameRegion = "";

	public int track = 0;

	public int payTrack = 0;

	public int getFirstLoginWeekDate() {
		return firstLoginWeekDate;
	}

	public void setFirstLoginWeekDate(int firstLoginWeekDate) {
		this.firstLoginWeekDate = firstLoginWeekDate;
	}

	public int getLastLoginWeekDate() {
		return lastLoginWeekDate;
	}

	public void setLastLoginWeekDate(int lastLoginWeekDate) {
		this.lastLoginWeekDate = lastLoginWeekDate;
	}

	public int getFirstPayWeekDate() {
		return firstPayWeekDate;
	}

	public void setFirstPayWeekDate(int firstPayWeekDate) {
		this.firstPayWeekDate = firstPayWeekDate;
	}

	public int getLastPayWeekDate() {
		return lastPayWeekDate;
	}

	public void setLastPayWeekDate(int lastPayWeekDate) {
		this.lastPayWeekDate = lastPayWeekDate;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getGameRegion() {
		return gameRegion;
	}

	public void setGameRegion(String gameRegion) {
		this.gameRegion = gameRegion;
	}

	public int getTrack() {
		return track;
	}

	public void setTrack(int track) {
		this.track = track;
	}

	public int getPayTrack() {
		return payTrack;
	}

	public void setPayTrack(int payTrack) {
		this.payTrack = payTrack;
	}

	public PlayerWeekInfo() {
		setFirstLoginWeekDate(firstLoginWeekDate);
		setLastLoginWeekDate(lastLoginWeekDate);
		setFirstPayWeekDate(firstPayWeekDate);
		setLastPayWeekDate(lastPayWeekDate);
		setChannel(channel);
		setGameRegion(gameRegion);
		setTrack(track);
		setPayTrack(payTrack);
	}

	public PlayerWeekInfo(int firstLoginWeekDate, int lastLoginWeekDate,
			int firstPayWeekDate, int lastPayWeekDate, String channel,
			String gameRegion, int track, int payTrack) {
		setFirstLoginWeekDate(firstLoginWeekDate);
		setLastLoginWeekDate(lastLoginWeekDate);
		setFirstPayWeekDate(firstPayWeekDate);
		setLastPayWeekDate(lastPayWeekDate);
		setChannel(channel);
		setGameRegion(gameRegion);
		setTrack(track);
		setPayTrack(payTrack);
	}

	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}

		PlayerWeekInfo t = (PlayerWeekInfo) o;
		return (JceUtil.equals(firstLoginWeekDate, t.firstLoginWeekDate)
				&& JceUtil.equals(lastLoginWeekDate, t.lastLoginWeekDate)
				&& JceUtil.equals(firstPayWeekDate, t.firstPayWeekDate)
				&& JceUtil.equals(lastPayWeekDate, t.lastPayWeekDate)
				&& JceUtil.equals(channel, t.channel)
				&& JceUtil.equals(gameRegion, t.gameRegion)
				&& JceUtil.equals(track, t.track) && JceUtil.equals(payTrack,
				t.payTrack));
	}

	public int hashCode() {
		try {
			throw new Exception("Need define key first!");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return 0;
	}

	public java.lang.Object clone() {
		java.lang.Object o = null;
		try {
			o = super.clone();
		} catch (CloneNotSupportedException ex) {
			assert false; // impossible
		}
		return o;
	}

	public void writeTo(JceOutputStream _os) {
		_os.write(firstLoginWeekDate, 0);
		_os.write(lastLoginWeekDate, 1);
		_os.write(firstPayWeekDate, 2);
		_os.write(lastPayWeekDate, 3);
		_os.write(channel, 4);
		_os.write(gameRegion, 5);
		_os.write(track, 6);
		_os.write(payTrack, 7);
	}

	public void readFrom(JceInputStream _is) {
		setFirstLoginWeekDate((int) _is.read(firstLoginWeekDate, 0, true));

		setLastLoginWeekDate((int) _is.read(lastLoginWeekDate, 1, true));

		setFirstPayWeekDate((int) _is.read(firstPayWeekDate, 2, true));

		setLastPayWeekDate((int) _is.read(lastPayWeekDate, 3, true));

		setChannel(_is.readString(4, true));

		setGameRegion(_is.readString(5, true));

		setTrack((int) _is.read(track, 6, true));

		setPayTrack((int) _is.read(payTrack, 7, true));

	}

	public void display(java.lang.StringBuilder _os, int _level) {
		JceDisplayer _ds = new JceDisplayer(_os, _level);
		_ds.display(firstLoginWeekDate, "firstLoginWeekDate");
		_ds.display(lastLoginWeekDate, "lastLoginWeekDate");
		_ds.display(firstPayWeekDate, "firstPayWeekDate");
		_ds.display(lastPayWeekDate, "lastPayWeekDate");
		_ds.display(channel, "channel");
		_ds.display(gameRegion, "gameRegion");
		_ds.display(track, "track");
		_ds.display(payTrack, "payTrack");
	}

}
