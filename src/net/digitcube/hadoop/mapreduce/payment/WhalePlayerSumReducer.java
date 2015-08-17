package net.digitcube.hadoop.mapreduce.payment;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeMap;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WhalePlayerSumReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, NullWritable> {

	private OutFieldsBaseModel redValObj = new OutFieldsBaseModel();
	private TreeMap<AmountKey, String[]> whalePlayerMap = new TreeMap<AmountKey, String[]>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		
		whalePlayerMap.clear();
		
		for(OutFieldsBaseModel val : values){
			String[] arr = val.getOutFields();
			//String accountId = arr[2];
			String accountId = arr[4];
			int payAmount = StringUtil.convertInt(arr[7], 0);
			
			AmountKey amount = new AmountKey(payAmount, accountId);
			if(whalePlayerMap.size() < 100){
				whalePlayerMap.put(amount, arr);
			}else if(null != whalePlayerMap.floorKey(amount)){
					whalePlayerMap.remove(whalePlayerMap.firstKey());
					whalePlayerMap.put(amount, arr);
			}
		}
		
		Collection<String[]> whaleplayers = whalePlayerMap.values();
		for(String[] whalePlayer : whaleplayers){
			redValObj.setOutFields(whalePlayer);
			redValObj.setSuffix(Constants.SUFFIX_WHALE_PLAYER_SUM);
			context.write(redValObj, NullWritable.get());
		}
	}

	/**
	 * 此类是为兼容不同玩家充值金额相同的情况
	 */
	public class AmountKey implements Comparable<AmountKey>{

		//不同玩家充值金额可能相同
		private int amount = 0;
		private String accoundId = "";
		
		private AmountKey(int amount, String accoundId) {
			this.amount = amount;
			this.accoundId = accoundId;
		}
		@Override
		public int compareTo(AmountKey o) {
			if(this.amount > o.amount){
				return 1;
			}if(this.amount == o.amount){
				//当两个玩家充值金额相同时，则比较它们的 accountId 来决定大小
				return this.accoundId.compareTo(o.accoundId);
			}else{
				return -1;
			}
		}
		@Override
		public int hashCode() {
			return (amount+accoundId).hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			AmountKey o = (AmountKey)obj;
			return this.amount == o.amount && this.accoundId.equals(accoundId);
		}
		@Override
		public String toString() {
			return amount+"";
		}
	}
}
