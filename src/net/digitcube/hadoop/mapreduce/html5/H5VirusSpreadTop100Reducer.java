package net.digitcube.hadoop.mapreduce.html5;

import java.io.IOException;
import java.util.TreeSet;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class H5VirusSpreadTop100Reducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	//保存传播  K 系数最大的 Top100 的 父 accountId
	private static final int spreadTopN = 100;
	private TreeSet<VirusSpreadInfo> spreadTop100Set = new TreeSet<VirusSpreadInfo>();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		spreadTop100Set.clear();
		
		int totalParentNodes = 0;
		int totalChildNodeS = 0;
		
		for (OutFieldsBaseModel val : values) {
			String[] arr = val.getOutFields();
			
			int i = 0;
			String parentAccountId = arr[i++];
			String childNodeCount = arr[i++];
			String totalLoginTimes = arr[i++];
			String totalOnlineTime = arr[i++];
			String totalPVs = arr[i++];
			String uniqIps = arr[i++];
			
			//20141010 与高境确认：目前暂只计算种子 K 系数
			if(MRConstants.INVALID_PLACE_HOLDER_CHAR.equals(parentAccountId)){
				//没有父节点，这里的子节点为种子节点，把子节点数记为 K 系数里的父节点数
				totalParentNodes += StringUtil.convertInt(childNodeCount, 0);
			}else{
				//有父节点，这里的子节点说明是病毒传播繁衍出来的节点，记为 K 系数里的子节点
				totalChildNodeS += StringUtil.convertInt(childNodeCount, 0);

				//病毒传播 TOP100 只计算父节点不是 "-" 的节点
				VirusSpreadInfo spredInfo = new VirusSpreadInfo(parentAccountId, childNodeCount, totalLoginTimes, totalOnlineTime, totalPVs, uniqIps);
				if(spreadTop100Set.size() < spreadTopN){
					spreadTop100Set.add(spredInfo);
				}else if(null != spreadTop100Set.floor(spredInfo)){
					spreadTop100Set.remove(spreadTop100Set.first());
					spreadTop100Set.add(spredInfo);
				}
			}
		}
		
		//输出传播系数  TOP100 的详情
		for(VirusSpreadInfo info : spreadTop100Set){
			String[] valFields = new String[]{
					info.parentAccountId,
					info.childNodeCount+"",
					info.totalLoginTimes+"",
					info.totalOnlineTime+"",
					info.totalPVs+"",
					info.uniqIps+""
			};
			
			valObj.setOutFields(valFields);
			key.setSuffix(Constants.SUFFIX_H5_VIRUS_SPREAD_TOP100);
			context.write(key, valObj);
		}
		
		//输出总的 K 系统数
		String[] valFields = new String[]{
				totalParentNodes + "",
				totalChildNodeS+""
		};
		valObj.setOutFields(valFields);
		key.setSuffix(Constants.SUFFIX_H5_VIRUS_SPREAD_KFACTOR);
		context.write(key, valObj);
	}

	public static class VirusSpreadInfo implements Comparable<VirusSpreadInfo>{
		
		String parentAccountId;
		int childNodeCount = 0;
		int totalLoginTimes;
		int totalOnlineTime;
		int totalPVs;
		int uniqIps;
		
		private VirusSpreadInfo(String parentAccountId, String childNodeCount, String totalLoginTimes,
				String totalOnlineTime, String totalPVs, String uniqIps) {
			this.parentAccountId = parentAccountId;
			this.childNodeCount += StringUtil.convertInt(childNodeCount, 0);
			this.totalLoginTimes = StringUtil.convertInt(totalLoginTimes, 0);
			this.totalOnlineTime = StringUtil.convertInt(totalOnlineTime, 0);
			this.totalPVs = StringUtil.convertInt(totalPVs, 0);
			this.uniqIps = StringUtil.convertInt(uniqIps, 0);
		}
		
		
		@Override
		public int compareTo(VirusSpreadInfo o) {
			if(this.childNodeCount > o.childNodeCount){
				return 1;
			}if(this.childNodeCount == o.childNodeCount){
				return this.parentAccountId.compareTo(o.parentAccountId);
			}else{
				return -1;
			}
		}
		
		@Override
		public int hashCode() {
			return (parentAccountId + childNodeCount + totalLoginTimes + totalOnlineTime + totalPVs + uniqIps).hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			VirusSpreadInfo o = (VirusSpreadInfo)obj;
			return this.parentAccountId == o.parentAccountId 
					&& this.childNodeCount == o.childNodeCount
					&& this.totalLoginTimes == o.totalLoginTimes
					&& this.totalOnlineTime == o.totalOnlineTime
					&& this.totalPVs == o.totalPVs
					&& this.uniqIps == o.uniqIps;
		}

		@Override
		public String toString() {
			return "VirusSpreadInfo [parentAccountId=" + parentAccountId
					+ ", childNodeCount=" + childNodeCount
					+ ", totalLoginTimes=" + totalLoginTimes
					+ ", totalOnlineTime=" + totalOnlineTime + ", totalPVs="
					+ totalPVs + ", uniqIps=" + uniqIps + "]\n";
		}
	}
	
	public static void main(String[] args){
		TreeSet<VirusSpreadInfo> spreadTop100Set = new TreeSet<VirusSpreadInfo>();
		
		int[] parentId = new int[]{10,2,3,41,11,5,8,9};
		for(int i : parentId){
			String parentAccountId = i + "";
			String value = i + "";
			VirusSpreadInfo spread = new VirusSpreadInfo(parentAccountId, value, value, value, value, value);
			
			if(spreadTop100Set.size() < 4){
				spreadTop100Set.add(spread);
			}else if(null != spreadTop100Set.floor(spread)){
				spreadTop100Set.remove(spreadTop100Set.first());
				spreadTop100Set.add(spread);
			}
			System.out.println(i+"=======================\n"+spreadTop100Set);
		}
	}
}
