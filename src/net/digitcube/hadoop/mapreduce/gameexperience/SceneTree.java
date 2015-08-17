package net.digitcube.hadoop.mapreduce.gameexperience;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.digitcube.hadoop.common.MD5Util;

public class SceneTree {
	
	    //唯一标识当前路径中当前节点，根节点 id 为 :_ROOT_ID
		private String sceneId; 
		//页面名称
		private String sceneName;
		//总浏览次数
		private int viewTimes;
		//总浏览时长（秒）
		private BigDecimal totalDuration;
		//在路径中的位置（深度）
		private int level;
		//父节点，永远只有一个
		private SceneTree parent;
		//子节点，零个或多个
		private Map<String, SceneTree> children;

		public SceneTree(SceneTree parent, String sceneName, BigDecimal duration){
			this.parent = parent;
			this.sceneName = sceneName;
			this.sceneId = getMD5Id();
			this.viewTimes++;
			this.totalDuration = duration;
			this.level = null == this.parent ? 0 : this.parent.level + 1;
			this.children = new TreeMap<String, SceneTree>();
			if(null != parent){
				this.parent.children.put(sceneName, this);
			}
		}
		
		public String getSceneId() {
			return sceneId;
		}
		public String getPageName() {
			return sceneName;
		}
		public int getViewTimes() {
			return viewTimes;
		}
		public BigDecimal getTotalDuration() {
			return totalDuration;
		}
		public int getLevel() {
			return level;
		}
		public SceneTree getParent() {
			return parent;
		}
		public Map<String, SceneTree> getChildren() {
			return children;
		}

		private String getMD5Id(){
			if(null == this.parent){
				return "_ROOT_ID";
			}
			if(null == this.sceneName){
				throw new RuntimeException("sceneName is null : " + this.toString());
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
				return this.sceneName + "," + this.parent.getAbsolutelyPath();
			}
			return this.sceneName;
		}
		
		public void getScenesInfo(List<SceneTree> tree, Collection<SceneTree> children){
			if(null == children || children.isEmpty()){
				return;
			}
			Set<SceneTree> t = new HashSet<SceneTree>();
			for(SceneTree scene : children){
				tree.add(scene);
				t.addAll(scene.children.values());
			}
			getScenesInfo(tree, t);
		}
		
		public SceneTree addPage(String sceneName, long duration){
			/*
			 * 20140714:即使前后两个页面的名称相同也算在下一步
			if(this.sceneName.equals(sceneName)){
				this.viewTimes++;
				this.totalDuration += duration;
				return this;
			}*/
			
			SceneTree child = this.children.get(sceneName);
			if(null != child){
				child.viewTimes++;
				child.totalDuration = child.totalDuration.add(new BigDecimal(duration));
				return child;
			}
			
			SceneTree newScene = new SceneTree(this, sceneName, new BigDecimal(duration));
			return newScene;
		}

		@Override
		public String toString() {
			return "(PageName="+this.sceneName + ", "
					 + "Parent="+this.parent.sceneName + ", "
					 + "ViewTimes="+this.viewTimes + ", "
					 + "totalDuration="+this.totalDuration + ", "
					 + "selfId="+this.sceneId + ", "
					 + "parentId="+ (null == this.parent ? null : this.parent.sceneId) + ", "
					 + ")";
		}
		
		public static void main(String[] args) {
			String[] a = {"1","3","2","5","4","6","2","5","7","9","8"};
			String[] b = {"1","3","2","5","4","6","6","8"};
			String[] c = {"1","4","5","7"};
			String[] d = {"1","4","5","7","2","3","6","8"};

			SceneTree root = new SceneTree(null, "ROOT", new BigDecimal(1));
			SceneTree tmp = root;
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
			
			List<SceneTree> tree = new ArrayList<SceneTree>();
			root.getScenesInfo(tree, root.children.values());
			int initLevel = root.level;
			for(SceneTree scene : tree){
				if(scene.level != initLevel){
					System.out.println("---------------------------------"+initLevel);
					initLevel = scene.level;
				}
				System.out.println(scene);
			}
		}
}
