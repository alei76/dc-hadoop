package net.digitcube.hadoop.common;

import java.util.LinkedHashMap;

/**
 * 按最近最少使用移除的 Map
 */
public class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5965404289604375598L;
	
	private final int maxCapacity;
	private static final float DEFAULT_LOAD_FACTOR = 0.75f;

	public LRULinkedHashMap(int maxCapacity) {
		super(maxCapacity, DEFAULT_LOAD_FACTOR, true);
		this.maxCapacity = maxCapacity;
	}

	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
		return size() > maxCapacity;
	}
}
