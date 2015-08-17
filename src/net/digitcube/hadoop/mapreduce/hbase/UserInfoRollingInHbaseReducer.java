package net.digitcube.hadoop.mapreduce.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.jce.PlayerInfoForHbase;
import net.digitcube.hadoop.jce.PlayerInfoMap;
import net.digitcube.hadoop.util.Base64Ext;
import net.digitcube.protocol.JceInputStream;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

public class UserInfoRollingInHbaseReducer extends
		TableReducer<Text, Text, ImmutableBytesWritable> {
	public void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		String k = key.toString();
		List<PlayerInfoForHbase> playerInfoForHbaseList = new ArrayList<PlayerInfoForHbase>();
		while (value.iterator().hasNext()) {
			String playerInfoForHbaseStr = value.iterator().next().toString();
			try {
				PlayerInfoForHbase playerInfoForHbase = new PlayerInfoForHbase();
				JceInputStream inputStream = new JceInputStream(
						Base64Ext.decode(playerInfoForHbaseStr));
				playerInfoForHbase.readFrom(inputStream);
				playerInfoForHbaseList.add(playerInfoForHbase);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (playerInfoForHbaseList.isEmpty()) {
			return;
		}
		Map<String, PlayerInfoForHbase> worldPlayerInfoMap = new HashMap<String, PlayerInfoForHbase>();
		for (PlayerInfoForHbase playerInfoForHbase : playerInfoForHbaseList) {
			if (MRConstants.ALL_GAMESERVER.equals(playerInfoForHbase
					.getGameRegion()) && playerInfoForHbaseList.size() == 2) {
				continue;
			}
			worldPlayerInfoMap.put(playerInfoForHbase.getGameRegion(),
					playerInfoForHbase);
		}
		PlayerInfoMap playerInfoMap = new PlayerInfoMap();
		playerInfoMap.setPlayerInfoMap(worldPlayerInfoMap);
		Put putrow = new Put(k.getBytes());
		putrow.add("info".getBytes(), "q".getBytes(),
				playerInfoMap.toByteArray());
		context.write(new ImmutableBytesWritable(key.getBytes()), putrow);
	}
}
