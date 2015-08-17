package net.digitcube.hadoop.mapreduce.adtrack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.digitcube.hadoop.common.OutFieldsBaseModel;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class ClickAndActiveReducer
		extends
		TableReducer<OutFieldsBaseModel, OutFieldsBaseModel, ImmutableBytesWritable> {

	private static final byte[] CF = Bytes.toBytes("info");
	private static final byte[] Q = Bytes.toBytes("q");
	
	public void reduce(OutFieldsBaseModel key,
			Iterable<OutFieldsBaseModel> values, Context context) {
		String appid = key.getOutFields()[0];
		String brand = key.getOutFields()[1];
		String os = key.getOutFields()[2];

		List<CompareAttr> actList = new ArrayList<CompareAttr>();
		Map<String, List<CompareAttr>> clickIPMap = new HashMap<String, List<CompareAttr>>();
		Map<String, List<CompareAttr>> provinceMap = new HashMap<String, List<CompareAttr>>();
		for (OutFieldsBaseModel val : values) {
			String[] outFields = val.getOutFields();
			CompareAttr compareAttr = new CompareAttr(outFields);
			if (compareAttr.isFromClick()) {
				List<CompareAttr> ipList = clickIPMap.get(compareAttr.getIp());
				if (ipList == null) {
					ipList = new ArrayList<CompareAttr>();
				}
				ipList.add(compareAttr);
				clickIPMap.put(compareAttr.getIp(), ipList);

				List<CompareAttr> provinceList = provinceMap.get(compareAttr
						.getIp());
				if (provinceList == null) {
					provinceList = new ArrayList<CompareAttr>();
				}
				provinceList.add(compareAttr);
				provinceMap.put(compareAttr.getProvince(), provinceList);

			} else {
				actList.add(compareAttr);
			}
		}

		if (actList.isEmpty()) {
			System.out.println("[Compare] actList Empty");
			return;
		}
		for (CompareAttr compareAttr : actList) {
			List<CompareAttr> list = clickIPMap.get(compareAttr.getIp());
			if (list != null) {
				compareAttr.setChannel(list.get(0).getChannel());
				write2Hbase(appid, context, compareAttr);
				System.out.println("[Compare] ip-compare-success\t" + appid
						+ "\t" + brand + "\t" + os + "\t"
						+ compareAttr.getUid() + "\t"
						+ compareAttr.getChannel() + "\t"
						+ compareAttr.getTimestamp() + "\t"
						+ list.get(0).getTimestamp());
				continue;
			}
			list = provinceMap.get(compareAttr.getProvince());
			if (list != null) {
				compareAttr.setChannel(list.get(0).getChannel());
				write2Hbase(appid, context, compareAttr);
				System.out.println("[Compare] province-compare-success\t"
						+ appid + "\t" + brand + "\t" + os + "\t"
						+ compareAttr.getUid() + "\t"
						+ compareAttr.getChannel() + "\t"
						+ compareAttr.getTimestamp() + "\t"
						+ list.get(0).getTimestamp() + "\t"
						+ compareAttr.getProvince() + "t" + compareAttr.getIp()
						+ "\t" + list.get(0).getIp());
				continue;
			}
			System.out.println("[Compare] province-compare-failed\t" + appid
					+ "\t" + brand + "\t" + os + "\t" + compareAttr.getUid()
					+ "\t" + compareAttr.getTimestamp() + "\t"
					+ compareAttr.getProvince() + "t" + compareAttr.getIp());
		}

	}

	private void write2Hbase(String appid, Context context,
			CompareAttr compareAttr) {
		byte[] hbaseKey = Bytes.toBytes(appid + "|" + compareAttr.getUid());
		Put putrow = new Put(hbaseKey);
		try {
			putrow.add(CF, Q, Bytes.toBytes(compareAttr.getChannel()));
			context.write(new ImmutableBytesWritable(hbaseKey), putrow);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
