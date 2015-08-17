package net.digitcube.hadoop.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.io.UnsupportedEncodingException;

public class PageTree {
	
	//唯一标识当前路径中当前节点，根节点 id 为 :_ROOT_ID
	private String pageId; 
	//页面名称
	private String pageName;
	//总浏览次数
	private int viewTimes;
	//总浏览时长（秒）
	private int totalDuration;
	//在路径中的位置（深度）
	private int level;
	//父节点，永远只有一个
	private PageTree parent;
	//子节点，零个或多个
	private Map<String, PageTree> children;

	public PageTree(PageTree parent, String pageName, int duration){
		this.parent = parent;
		this.pageName = pageName;
		this.pageId = getMD5Id();
		this.viewTimes++;
		this.totalDuration = duration;
		this.level = null == this.parent ? 0 : this.parent.level + 1;
		this.children = new TreeMap<String, PageTree>();
		if(null != parent){
			this.parent.children.put(pageName, this);
		}
	}
	
	public String getPageId() {
		return pageId;
	}
	public String getPageName() {
		return pageName;
	}
	public int getViewTimes() {
		return viewTimes;
	}
	public int getTotalDuration() {
		return totalDuration;
	}
	public int getLevel() {
		return level;
	}
	public PageTree getParent() {
		return parent;
	}
	public Map<String, PageTree> getChildren() {
		return children;
	}

	private String getMD5Id(){
		if(null == this.parent){
			return "_ROOT_ID";
		}
		if(null == this.pageName){
			throw new RuntimeException("PageName is null : " + this.toString());
		}
		try {
			return MD5Util.getMD5Str(this.getAbsolutelyPath());
			//String tmp = this.getPageNames();
			//return tmp + ", " + MD5Util.getMD5Str(tmp);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("getMD5id-->UnsupportedEncodingException : " + e.getStackTrace());
		}
	}
	//页面访问的全路径（逗号分割，逆向）
	private String getAbsolutelyPath(){
		if(null != this.parent){
			return this.pageName + "," + this.parent.getAbsolutelyPath();
		}
		return this.pageName;
	}
	
	public void getPagesInfo(List<PageTree> tree, Collection<PageTree> children){
		if(null == children || children.isEmpty()){
			return;
		}
		Set<PageTree> t = new HashSet<PageTree>();
		for(PageTree page : children){
			tree.add(page);
			t.addAll(page.children.values());
		}
		getPagesInfo(tree, t);
	}
	
	public PageTree addPage(String pageName, int duration){
		/*
		 * 20140714:即使前后两个页面的名称相同也算在下一步
		if(this.pageName.equals(pageName)){
			this.viewTimes++;
			this.totalDuration += duration;
			return this;
		}*/
		
		PageTree child = this.children.get(pageName);
		if(null != child){
			child.viewTimes++;
			child.totalDuration += duration;
			return child;
		}
		
		// Added at 20150112
		// 有异常数据时子节点路径达到近 10w 级那么深
		// 所以这里做规避，当深度达到 1k 时就不再往下建子节点
		// 理论上很少有需要看到这么深的页面访问路径
		if(this.level >= 1000){
			return this;
		}
				
		PageTree newPage = new PageTree(this, pageName, duration);
		return newPage;
	}

	@Override
	public String toString() {
		return "(PageName="+this.pageName + ", "
				 + "Parent="+this.parent.pageName + ", "
				 + "ViewTimes="+this.viewTimes + ", "
				 + "totalDuration="+this.totalDuration + ", "
				 + "selfId="+this.pageId + ", "
				 + "parentId="+ (null == this.parent ? null : this.parent.pageId) + ", "
				 + ")";
	}
	
	public static void main(String[] args) {
		String[] a = {"1","3","2","5","4","6","2","5","7","9","8"};
		String[] b = {"1","3","2","5","4","6","6","8"};
		String[] c = {"1","4","5","7"};
		String[] d = {"1","4","5","7","2","3","6","8"};

		PageTree root = new PageTree(null, "ROOT", 1);
		PageTree tmp = root;
		for(String s : a){
			tmp = tmp.addPage(s, 1);
		}
		tmp = root;
		for(String s : b){
			tmp = tmp.addPage(s, 1);
		}
		tmp = root;
		for(String s : c){
			tmp = tmp.addPage(s, 1);
		}
		tmp = root;
		for(String s : d){
			tmp = tmp.addPage(s, 1);
		}
		
		List<PageTree> tree = new ArrayList<PageTree>();
		root.getPagesInfo(tree, root.children.values());
		int initLevel = root.level;
		for(PageTree page : tree){
			if(page.level != initLevel){
				System.out.println("---------------------------------"+initLevel);
				initLevel = page.level;
			}
			System.out.println(page);
		}
	}

}
