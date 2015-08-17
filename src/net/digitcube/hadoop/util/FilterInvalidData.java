package net.digitcube.hadoop.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import net.digitcube.hadoop.common.MRConstants;

/**
 * 离群数据剔除算法 
 * @author rickpan
 */
public class FilterInvalidData {

	static int factor = 1;
	static float threshold = 0.993f;
	
	static RangeCount tmpLeft = null;
	static RangeCount tmpRight = null;
	static RangeCount finalLeft = null;
	static RangeCount finalRight = null;
	
	public static void main(String[] args) throws Exception {
		//A. 初始化训练数据群
		//TreeSet<Integer> source = initSource4Set();
		//TreeMap<Integer, Integer> source = initSource4Map("C:\\Users\\Administrator\\Desktop\\appDownloadTimeSource");
		//List<Integer> source = initSource4List("C:\\Users\\Administrator\\Desktop\\appDownloadTimeSource_sorted");
		//List<Integer> source = initSource4List("E:\\TCL 异常数据剔除\\upBytesSource_sorted");
		List<Integer> source = initSource4List("E:\\TCL 异常数据剔除\\downBytesSource_sorted");
		
		
		//System.out.println(source);
		//B. 初始化区间并计算每个区间的数据个数
		//TreeMap<Integer, RangeCount> rangeCountmap = rangeCount4Set(factor, source);
		//TreeMap<Integer, RangeCount> rangeCountmap = rangeCount4Map(factor, source);
		TreeMap<Integer, RangeCount> rangeCountmap = rangeCount4List(factor, source);
		//System.out.println(rangeCountmap);
		//C. 找出有效数值的上限及下限
		findTargetRange(source.size(), threshold, rangeCountmap);
		//D. 打印有效数值的上限及下限
		printDetail(source.size(), rangeCountmap);
	}
	
	/**
	 * 初始化源数据
	 * @return
	 */
	private static TreeSet<Integer> initSource4Set(){
		TreeSet<Integer> source = new TreeSet<Integer>();
		Random r = new Random();
		while(source.size()<1000001){
			source.add(r.nextInt(Integer.MAX_VALUE));
		}
		
		/*while(source.size()<11){
			source.add(r.nextInt(100));
		}*/
		/*测试
		int[] a = {10, 11, 12, 23, 30, 41, 43, 81, 82, 87, 98};
		for(int i : a){
			source.add(i);
		}*/
		return source;
	}
	
	private static TreeMap<Integer, Integer> initSource4Map(String fileName) throws Exception{
		TreeMap<Integer, Integer> source = new TreeMap<Integer, Integer>();
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		while(null != (line = br.readLine())){
			String[] arr = line.split(MRConstants.SEPERATOR_IN);
			int duration = StringUtil.convertInt(arr[0], 0);
			int times = StringUtil.convertInt(arr[1], 0);
			source.put(duration, times);
		}
		br.close();
		return source;
	}
	
	private static List<Integer> initSource4List(String fileName) throws Exception{
		List<Integer> source = new ArrayList<Integer>();
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		while(null != (line = br.readLine())){
			String[] arr = line.split(MRConstants.SEPERATOR_IN);
			int item = StringUtil.convertInt(arr[0], 0);
			int count = StringUtil.convertInt(arr[1], 0);
			if(0 == item){
				continue;
			}
			for(int i=0; i<count; i++){
				source.add(item);
			}
		}
		br.close();
		return source;
	}
	
	/**
	 * 对 source 中每个数据，把它分配到特定的区间
	 * 同时，计算每个区间分配到的数据个数
	 * @param source
	 * @return
	 */
	private static TreeMap<Integer, RangeCount> rangeCount4Set(int factor, TreeSet<Integer> source){
		// 目标数据集的最大最小值
		int min = source.first();
		int max = source.last();
		
		// 把目标数据集分为 steps 个区间, 每个区间大小为 interval
		int steps = source.size()*factor;
		int interval = (max-min)/steps;
		System.out.println("min->"+min+",max->"+max+",total->"+source.size()+",steps->"+steps+",interval->"+interval);
		
		// 对 steps 个区间，以 interval 为步长初始化区间数组
		int[] rangeArr = ((min + interval*steps) < max) ? new int[steps+1] : new int[steps];
		for(int i=0; i<steps; i++){
			// 以 interval 为步长初始化区间数组
			rangeArr[i] = min + interval*(1+i);
		}
		if((min + interval*steps) < max){
			rangeArr[steps] = max;
		}
		
		/*
		 * 计算中位数极其所在区间（下一步查找目标数据集时将从中位数所在区间开始）
		 * 同时计算每个区间分配到的数据个数及该区间的最大最小值 
		 */
		int nidex = 0;
		int midNum = 0;
		int midIndex = source.size()/2;
		TreeMap<Integer, RangeCount> rangeCountMap = new TreeMap<Integer, RangeCount>();
		for(int i : source){
			if(nidex == midIndex ){
				midNum = i; // 中位数
				System.out.println("mid="+midNum);
			}
			nidex++;
			
			// 计算当前数所属的区间
			int range = getRangeTop(i, rangeArr);
			
			// 计算每个区间分配到的数据个数及该区间的最大最小值
			RangeCount p = rangeCountMap.get(range);
			if(null == p){
				p = new RangeCount(range, 1, i, i);
				rangeCountMap.put(p.range, p);
			}else{
				if(i < p.min){
					p.min = i;
				}
				if(i > p.max){
					p.max = i;
				}
				p.count++;
			}
		}
		
		// 把查找目标数据偏移索引设置为从中位数开始
		Integer midNumRange = getRangeTop(midNum, rangeArr);
		tmpLeft = rangeCountMap.get(midNumRange);
		Integer ceilingRange = rangeCountMap.ceilingKey(midNumRange + 1);
		if(null != ceilingRange){
			tmpRight = rangeCountMap.get(ceilingRange);
		}
		
		return rangeCountMap;
	}
	
	private static TreeMap<Integer, RangeCount> rangeCount4Map(int factor, TreeMap<Integer, Integer> source){
		// 目标数据集的最大最小值
		int min = source.firstKey();
		int max = source.lastKey();
		
		// 把目标数据集分为 steps 个区间, 每个区间大小为 interval
		int steps = source.size()*factor;
		int interval = (max-min)/steps;
		System.out.println("min->"+min+",max->"+max+",total->"+source.size()+",steps->"+steps+",interval->"+interval);
		
		// 对 steps 个区间，以 interval 为步长初始化区间数组
		int[] rangeArr = ((min + interval*steps) < max) ? new int[steps+1] : new int[steps];
		for(int i=0; i<steps; i++){
			// 以 interval 为步长初始化区间数组
			rangeArr[i] = min + interval*(1+i);
		}
		if((min + interval*steps) < max){
			rangeArr[steps] = max;
		}
		
		/*
		 * 计算中位数极其所在区间（下一步查找目标数据集时将从中位数所在区间开始）
		 * 同时计算每个区间分配到的数据个数及该区间的最大最小值 
		 */
		int index = 0;
		int midNum = 0;
		int midIndex = source.size()/2;
		TreeMap<Integer, RangeCount> rangeCountMap = new TreeMap<Integer, RangeCount>();
		for(Entry<Integer, Integer> i : source.entrySet()){
			if(index == midIndex ){
				midNum = i.getKey(); // 中位数
				System.out.println("mid="+midNum);
			}
			index++;
			
			// 计算当前数所属的区间
			int range = getRangeTop(i.getKey(), rangeArr);
			
			// 计算每个区间分配到的数据个数及该区间的最大最小值
			RangeCount p = rangeCountMap.get(range);
			if(null == p){
				p = new RangeCount(range, 1, i.getKey(), i.getKey());
				rangeCountMap.put(p.range, p);
			}else{
				if(i.getKey() < p.min){
					p.min = i.getKey();
				}
				if(i.getKey() > p.max){
					p.max = i.getKey();
				}
				p.count += i.getValue();
			}
		}
		
		// 把查找目标数据偏移索引设置为从中位数开始
		Integer midNumRange = getRangeTop(midNum, rangeArr);
		tmpLeft = rangeCountMap.get(midNumRange);
		Integer ceilingRange = rangeCountMap.ceilingKey(midNumRange + 1);
		if(null != ceilingRange){
			tmpRight = rangeCountMap.get(ceilingRange);
		}
		
		return rangeCountMap;
	}
	
	private static TreeMap<Integer, RangeCount> rangeCount4List(int factor, List<Integer> source){
		// 排序
		//Collections.sort(source);
		
		// 目标数据集的最大最小值
		int min = source.get(0);
		int max = source.get(source.size() - 1);
		
		// 把目标数据集分为 steps 个区间, 每个区间大小为 interval
		int steps = source.size()*factor;
		int interval = (max-min)/steps;
		System.out.println("min->"+min+",max->"+max+",total->"+source.size()+",steps->"+steps+",interval->"+interval);
		
		// 对 steps 个区间，以 interval 为步长初始化区间数组
		int[] rangeArr = ((min + interval*steps) < max) ? new int[steps+1] : new int[steps];
		for(int i=0; i<steps; i++){
			// 以 interval 为步长初始化区间数组
			rangeArr[i] = min + interval*(1+i);
		}
		if((min + interval*steps) < max){
			rangeArr[steps] = max;
		}
		
		/*
		 * 计算中位数极其所在区间（下一步查找目标数据集时将从中位数所在区间开始）
		 * 同时计算每个区间分配到的数据个数及该区间的最大最小值 
		 */
		int index = 0;
		int midNum = 0;
		int midIndex = source.size()/2;
		TreeMap<Integer, RangeCount> rangeCountMap = new TreeMap<Integer, RangeCount>();
		for(Integer i : source){
			if(index == midIndex ){
				midNum = i; // 中位数
				System.out.println("mid="+midNum);
			}
			index++;
			
			// 计算当前数所属的区间
			int range = getRangeTop(i, rangeArr);
			
			// 计算每个区间分配到的数据个数及该区间的最大最小值
			RangeCount p = rangeCountMap.get(range);
			if(null == p){
				p = new RangeCount(range, 1, i, i);
				rangeCountMap.put(p.range, p);
			}else{
				if(i < p.min){
					p.min = i;
				}else if(i > p.max){
					p.max = i;
				}
				p.count++;
			}
		}
		
		// 把查找目标数据偏移索引设置为从中位数开始
		Integer midNumRange = getRangeTop(midNum, rangeArr);
		tmpLeft = rangeCountMap.get(midNumRange);
		Integer ceilingRange = rangeCountMap.ceilingKey(midNumRange + 1);
		if(null != ceilingRange){
			tmpRight = rangeCountMap.get(ceilingRange);
		}
		
		return rangeCountMap;
	}
	
	/**
	 * 从中位数开始查找目标数据集，循环查找，直到
	 * 目标数据集大小/total >= threshold 时停止
	 * @param total
	 * @param threshold
	 * @param rangeCountmap
	 */
	private static void findTargetRange(int total, float threshold, TreeMap<Integer, RangeCount> rangeCountmap){
		int count = 0;
		while(true){
			// 优先从向左方向取数
			if(null != tmpLeft){
				finalLeft = tmpLeft;
				
				count += tmpLeft.count;
				if(((float)count)/((float)total) >= threshold){
					break;
				}
				
				// 当左边界达到下限时，不在继续向左边界取数
				Entry<Integer, RangeCount> floor = rangeCountmap.floorEntry(tmpLeft.range - 1);
				if(null != floor){
					tmpLeft = floor.getValue();
				}else{
					tmpLeft = null;
				}
			}
			
			// 当向左方向所取数个数未达到目标值时, 继续向右方向取数
			if(null != tmpRight){
				finalRight = tmpRight; 
				count += tmpRight.count;
				if(((float)count)/((float)total) >= threshold){
					break;
				}
				
				// 当右边界达到上限时，不在继续向右边界取数
				Entry<Integer, RangeCount> ceiling = rangeCountmap.ceilingEntry(tmpRight.range + 1);
				if(null != ceiling){
					tmpRight = ceiling.getValue();
				}else{
					tmpRight = null;
				}
			}
		}
	}

	/**
	 * 
	 * @param total
	 * @param map
	 */
	private static void printDetail(int total, TreeMap<Integer, RangeCount> map){
		if(null == finalRight){
			finalRight = finalLeft;
		}
		System.out.println("min="+finalLeft.min);
		System.out.println("max="+finalRight.max);
		
		int targetCount = 0;
		int rangeCount = 0;
		for(RangeCount cnt : map.values()){
			if(finalLeft.min <= cnt.min && cnt.max <= finalRight.max){
				rangeCount++;
				targetCount+=cnt.count;
			}
		}
		System.out.println("total="+total+",targetCount="+targetCount+",rangeCount="+rangeCount);
		
		// 打印向左方向的无效数据
		Map<Integer, RangeCount> leftInvalidMap = map.headMap(finalLeft.range);
		//System.out.println("leftInvalidMap="+leftInvalidMap);
		if(null == leftInvalidMap || leftInvalidMap.isEmpty()){
			System.out.println("All the data of left side are valid, min="+finalLeft.min);
		}else{
			int max = 0;
			for(RangeCount cnt : leftInvalidMap.values()){
				if(cnt.max > max){
					max = cnt.max; 
				}
				//System.out.println(cnt);
			}
			System.out.println("left side invalid data count="+leftInvalidMap.size() + ", max="+max);
		}
		
		// 打印向右方向的无效数据
		Map<Integer, RangeCount> rightIvalidMap = map.tailMap(finalRight.range+1);
		//System.out.println("rightIvalidMap="+rightIvalidMap);
		if(null == rightIvalidMap || rightIvalidMap.isEmpty()){
			System.out.println("All the data of right side are valid, min="+finalRight.max);
		}else{
			int min = Integer.MAX_VALUE;
			for(RangeCount cnt : rightIvalidMap.values()){
				if(cnt.min < min){
					min = cnt.min; 
				}
				//System.out.println(cnt);
			}
			System.out.println("right side invalid data count="+rightIvalidMap.size()+", min="+min);
		}
	}
	
	/**
	 * 检查当期数据集大小是否达到目标
	 * 
	 * @param total：数据集总大小
	 * @param threshold：目标大小
	 * @param left：当期查找到有效数据集下边界
	 * @param right：当期查找到有效数据集上边界
	 * @return
	 */
	/*private static boolean checkThreshold(int total, float threshold, RangeCount left, RangeCount right){
		if(null != left){
			count += left.count;
			if(((float)count)/((float)total) >= threshold){
				return true;
			}
		}
		
		if(null != right){
			count += right.count;
			if(((float)count)/((float)total) >= threshold){
				return true;
			}
		}
		
		return false;
	}*/
	
	/**
	 * 给定一个数值和升序数组
	 * 用二叉查找法查找其在数组中本应所属的位置
	 * @param val
	 * @param rangeArr：升序数组
	 * @return
	 */
	private static int getRangeTop(int val, int[] rangeArr) {
		int result = Arrays.binarySearch(rangeArr, val);
		int index = Math.abs(result < 0 ? result + 1 : result);
		return rangeArr[index];
	}
	
	static class RangeCount{
		int range;
		int count;
		int min;
		int max;
		
		private RangeCount(int range, int count, int min, int max) {
			this.range = range;
			this.count = count;
			this.min = min;
			this.max = max;
		}

		@Override
		public String toString() {
			return "RangeCount [range=" + range + ", count=" + count + ", min="
					+ min + ", max=" + max + "]";
		}
		
	}

}
