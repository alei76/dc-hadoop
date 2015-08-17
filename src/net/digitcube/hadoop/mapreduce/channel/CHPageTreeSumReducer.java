package net.digitcube.hadoop.mapreduce.channel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.common.PageTree;
import net.digitcube.hadoop.util.StringUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * <pre>
 * 主要逻辑： 对所有玩家同一次登录排序好的访问页面构建一棵页面访问树
 * 统计每个页面的访问次数及平均时长
 * 输入
 * @see CHPageTreeSumMapper
 * 
 * 输出
 * Key				appId,appVersion,channel,country,province
 * KeySuffix		Constants.SUFFIX_CHANNEL_PAGETREE_SUM
 * Value:			pageName,pageID,parentName,parentID,viewTimes,avgDuration
 * @author sam.xie
 * @date 2015年3月20日 下午6:57:01
 * @version 1.0
 */
public class CHPageTreeSumReducer extends Reducer<OutFieldsBaseModel, Text, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel valObj = new OutFieldsBaseModel();

	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		// 构建页面访问树，对于不同的 key 分别构建一棵完整的树
		PageTree root = new PageTree(null, "ROOT", 0);

		for (Text val : values) {
			// 对每次登录访问的页面必须从根节点开始构建
			PageTree currentNode = root;
			String[] pages = val.toString().split(",");
			for (String page : pages) {
				String[] arr = page.split(":");
				String pageName = arr[0];
				int duration = StringUtil.convertInt(arr[1], 0);
				// 构建，返回当前节点
				currentNode = currentNode.addPage(pageName, duration);
			}
		}

		// 获取 root 的所有子节点（不包括 root 节点）
		List<PageTree> pagesInfo = new ArrayList<PageTree>();
		root.getPagesInfo(pagesInfo, root.getChildren().values());
		// 输出每个页面的访问信息
		for (PageTree page : pagesInfo) {
			int avgDuration = page.getTotalDuration() / page.getViewTimes();
			String[] valFileds = new String[] { page.getPageName(), page.getPageId(), page.getParent().getPageName(),
					page.getParent().getPageId(), page.getViewTimes() + "", avgDuration + "" };
			valObj.setOutFields(valFileds);
			key.setSuffix(Constants.SUFFIX_CHANNEL_PAGETREE_SUM);
			context.write(key, valObj);
		}
	}
}
