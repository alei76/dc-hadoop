package net.digitcube.hadoop.mapreduce.ext.roll;

public class ItemInfo {

	private String itemId;
	private String itemType;
	private int buyCount;
	private int getCount;
	private int useCount;
	
	public String getItemId() {
		return itemId;
	}
	public void setItemId(String itemId) {
		this.itemId = itemId;
	}
	public String getItemType() {
		return itemType;
	}
	public void setItemType(String itemType) {
		this.itemType = itemType;
	}
	public int getBuyCount() {
		return buyCount;
	}
	public void setBuyCount(int buyCount) {
		this.buyCount = buyCount;
	}
	public int getGetCount() {
		return getCount;
	}
	public void setGetCount(int getCount) {
		this.getCount = getCount;
	}
	public int getUseCount() {
		return useCount;
	}
	public void setUseCount(int useCount) {
		this.useCount = useCount;
	}
	
	@Override
	public String toString() {
		return "ItemInfo [itemId=" + itemId + ", itemType=" + itemType
				+ ", buyCount=" + buyCount + ", getCount=" + getCount
				+ ", useCount=" + useCount + "]";
	}
}
