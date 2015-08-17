package test.exception;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

public class CheckException {

	static int total = 0;
	static float threshold = 0.85f;
	public static void main(String[] args) {
		TreeSet<Integer> set = new TreeSet<Integer>();
		while(set.size()<10001){
			set.add(new Random().nextInt(90000000));
		}
		//System.out.println(set);
		total = set.size();
		
		int min = set.first();
		int max = set.last();
		int steps = total*100;
		int interval = (max-min)/steps;
		System.out.println("min->"+min+",max->"+max+",total->"+total+",steps->"+steps+",interval->"+interval);
		
		int[] rangeArr = ((min + interval*steps) < max) ? new int[steps+1] : new int[steps];
		for(int i=0; i<steps; i++){
			rangeArr[i] = min + interval*(1+i);
		}
		if((min + interval*steps) < max){
			rangeArr[steps] = max;
		}
		
		int midNum = 0;
		int midIndex = 0;
		TreeMap<Integer, RangeCount> map = new TreeMap<Integer, RangeCount>();
		for(int i : set){
			if(midIndex == total/2){
				midNum = i;
			}
			midIndex++;
			
			int range = getRangeTop(i, rangeArr);
			RangeCount p = map.get(range);
			if(null == p){
				p = new RangeCount(range, 1, i, i);
				map.put(p.range, p);
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
		System.out.println(midNum);
		/*for(RangeCount cnt : map.values()){
			System.out.println(cnt);
		}*/
		
		int midNumRange = getRangeTop(midNum, rangeArr);
		left = map.get(midNumRange);
		left_min = left.min;
		Integer ceiling = map.ceilingKey(midNumRange + 1);
		right = map.get(ceiling);
		right_max = null == right ? left.max : right.max;
		while(!findRange(left, right, map)){
			System.out.println("while-->"+depth);
			System.out.println("left-->"+left);
			System.out.println("right-->"+right);
		}
		System.out.println("min="+left_min);
		System.out.println("max="+right_max);
		
		int rangeNum = 0;
		int itemCount = 0;
		for(RangeCount cnt : map.values()){
			if(left_min <= cnt.min && cnt.max <= right_max){
				itemCount++;
				rangeNum+=cnt.count;
			}
		}
		System.out.println("total="+total+",rangeNum="+rangeNum+",itemCount="+itemCount);
	}

	static int depth = 0;
	static int count = 0;
	static int left_min = 0;
	static int right_max = 0;
	static RangeCount left = null;
	static RangeCount right = null;
	private static boolean findRange(RangeCount leftTmp, RangeCount rightTmp, TreeMap<Integer, RangeCount> map){
		if(depth > 50){
			return false;
		}
		depth++;
		
		if(null != leftTmp){
			left_min = leftTmp.min;
			count += leftTmp.count;
			if(checkGood(count)){
				return true;
			}
		}
		
		if(null != rightTmp){
			right_max = rightTmp.max;
			count += rightTmp.count;
			if(checkGood(count)){
				return true;
			}
		}
		
		if(null != leftTmp){
			Entry<Integer, RangeCount> floor = map.floorEntry(left.range - 1);
			if(null != floor){
				left = floor.getValue();
			}
		}
		
		if(null != rightTmp){
			Entry<Integer, RangeCount> ceiling = map.ceilingEntry(right.range + 1);
			if(null != ceiling){
				right = ceiling.getValue();
			}
		}
		
		return findRange(left, right, map);
	}
	
	private static boolean checkGood(int count){
		if(((float)count)/((float)total) >= threshold){
			return true;
		}
		return false;
	}
	
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