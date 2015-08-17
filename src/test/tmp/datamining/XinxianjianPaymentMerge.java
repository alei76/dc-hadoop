package test.tmp.datamining;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.mapreduce.domain.UserInfoRollingLog;
import net.digitcube.hadoop.model.EventLog;
import net.digitcube.hadoop.model.warehouse.WHPaymentLog;
import net.digitcube.hadoop.util.StringUtil;

/**
 * 将日志合并成报表
 * 
 * @author sam.xie
 * @date 2015年7月08日 下午20:57:33
 * @version 1.0
 */
public class XinxianjianPaymentMerge {

	public static void main(String[] args) throws IOException {
		XinxianjianPaymentMerge merge = new XinxianjianPaymentMerge();
		// merge.resolve_Coin_Gain();
		// merge.resolve_Coin_Lost();
		// merge.resolve_ItemGet();
		// merge.resolve_ItemBuy();
		// merge.resolve_ItemUse();
		merge.what();
	}

	public void what() throws IOException {
		String date_newuser = ".201504";
		String date_payment = ".201507";
		String file_newuser = "D:\\datatask\\xinxianjian\\userrolling" + date_newuser;
		String file_payment = "D:\\datatask\\xinxianjian\\wh_payment_day" + date_payment;
		String resultFile = "D:\\datatask\\xinxianjian\\新增用户渠道付费_新增(" + date_newuser +")_付费(" + date_payment + ").csv";
		BufferedReader reader = new BufferedReader(new FileReader(file_payment));
		String line = "";
		Map<String, Float> accountPaymentMap = new HashMap<String, Float>();
		Map<String, Float> channelPaymentMap = new HashMap<String, Float>();
		while ((line = reader.readLine()) != null) {
			WHPaymentLog log = new WHPaymentLog(line.split("\t"));
			String accountId = log.getAccountId();
			float amount = log.getCurrencyAmount();
			Float totalAmount = accountPaymentMap.get(accountId);
			totalAmount = (totalAmount == null) ? amount : totalAmount + amount;
			accountPaymentMap.put(accountId, totalAmount);
		}
		reader.close();
		reader = new BufferedReader(new FileReader(new File(file_newuser)));
		while ((line = reader.readLine()) != null) {
			UserInfoRollingLog log = new UserInfoRollingLog(new Date(), line.split("\t"));
			if (log.getPlayerDayInfo().getGameRegion().equals("_ALL_GS")) {
				String channel = log.getPlayerDayInfo().getChannel();
				String accountId = log.getAccountID();
				Float totalAmount = channelPaymentMap.get(channel);
				totalAmount = (totalAmount == null) ? 0 : totalAmount;
				if (accountPaymentMap.get(accountId) != null) {
					totalAmount += accountPaymentMap.get(accountId);
				}
				channelPaymentMap.put(channel, totalAmount);
			}
		}
		reader.close();
		BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile));
		writer.write("");
		writer.append("渠道,金额\n");
		if (channelPaymentMap.size() > 0) {
			for (Entry<String, Float> entry : channelPaymentMap.entrySet()) {
				if (entry.getValue() > 0) {
					System.out.println(entry.getKey() + "," + entry.getValue());
					writer.append(entry.getKey() + ", " + entry.getValue() + "\n");
				}
			}
		}
		writer.close();

		System.out.println(channelPaymentMap.size());
	}

}
