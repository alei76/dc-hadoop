package net.digitcube.hadoop.mapreduce.gameexperience;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SceneTreeSumReducer extends Reducer<OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//构建页面访问树，对于不同的 key 分别构建一棵完整的树
		SceneTree root = new SceneTree(null, "ROOT", new BigDecimal(0));
		
		for(Text val : values){
			//对每次登录访问的页面必须从根节点开始构建
			SceneTree currentNode = root;
			String[] scenes = val.toString().split(",");
			for(String scene : scenes){
				String[] arr = scene.split(":");
				String sceneName = arr[0];
				long duration = StringUtil.convertLong(arr[1], 0);
				//构建，返回当前节点
				currentNode = currentNode.addPage(sceneName, duration);
			}
		}
		
		//获取 root 的所有子节点（不包括 root 节点）
		List<SceneTree> scenesInfo = new ArrayList<SceneTree>();
		root.getScenesInfo(scenesInfo, root.getChildren().values());
		//输出每个页面的访问信息
		for(SceneTree scene : scenesInfo){
			//long avgDuration = scene.getTotalDuration().divide(new BigDecimal(scene.getViewTimes())).toBigInteger().longValue();
			BigDecimal duration = scene.getTotalDuration().divide(new BigDecimal(scene.getViewTimes()),0, BigDecimal.ROUND_CEILING);
			long avgDuration = duration.toBigInteger().longValue();
			
			// 毫秒转为秒
			avgDuration = avgDuration/1000;
			String[] valFileds = new String[]{
				scene.getPageName(),
				scene.getSceneId(),
				scene.getParent().getSceneId(),
				scene.getViewTimes() + "",
				avgDuration + ""
			};
			valObj.setOutFields(valFileds);
			key.setSuffix(Constants.SUFFIX_APP_SCENETREE_SUM);
			context.write(key, valObj);
		}
	}

}
