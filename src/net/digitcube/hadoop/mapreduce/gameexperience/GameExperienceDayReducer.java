package net.digitcube.hadoop.mapreduce.gameexperience;

import java.io.IOException;
import java.math.BigDecimal;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import net.digitcube.hadoop.util.StringUtil;

import org.apache.hadoop.mapreduce.Reducer;

public class GameExperienceDayReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {
	
	@Override
	protected void reduce(OutFieldsBaseModel key,Iterable<OutFieldsBaseModel> values, Context context) 
					throws IOException, InterruptedException {		
		
		if(key.getSuffix().equals(Constants.SUFFIX_SCENEINFO_DURATIONRXRT)){
			// 场景总时长 、上行、下行
			OutFieldsBaseModel outputValue = new OutFieldsBaseModel();
			BigDecimal duration = new BigDecimal(0);
			BigDecimal networkTxFlow = new BigDecimal(0);
			BigDecimal networkRxFlow = new BigDecimal(0);
			String orientation = "";
			long i = 0;
			for (OutFieldsBaseModel value : values) {
				String[] array = value.getOutFields();
				duration = duration.add(StringUtil.convertBigDecimal(array[0], 0));
				networkTxFlow = networkTxFlow.add(StringUtil.convertBigDecimal(array[1],0));
				networkRxFlow = networkRxFlow.add(StringUtil.convertBigDecimal(array[2],0));
				orientation = array[3];
				i++;
			}	
			outputValue.setOutFields(new String[]{duration.toPlainString(),networkTxFlow.toPlainString(),networkRxFlow.toPlainString(),String.valueOf(i),orientation});
			context.write(key, outputValue);
		}else if(key.getSuffix().equals(Constants.SUFFIX_SCENEINFO_GESTURE)){
			// 场景手势
			OutFieldsBaseModel outputValue = new OutFieldsBaseModel();
			long sum = 0;		
			String orientation = "";
			for (OutFieldsBaseModel value : values) {
				String[] array = value.getOutFields();
				sum += StringUtil.convertLong(array[0], 0);
				orientation = array[1];
			}
			outputValue.setOutFields(new String[]{String.valueOf(sum),orientation});
			context.write(key, outputValue);
		}else if(key.getSuffix().equals(Constants.SUFFIX_SCENEINFO_TOUCH)){
			// 热力图
			long sum = 0;
			String orientation = "";
			OutFieldsBaseModel outputValue = new OutFieldsBaseModel();
			for (OutFieldsBaseModel value : values) {
				String[] array = value.getOutFields();
				sum += StringUtil.convertLong(array[0],0);
				orientation = array[1];				
			}
			outputValue.setOutFields(new String[]{String.valueOf(sum),orientation});
			context.write(key, outputValue);
		}else if(key.getSuffix().equals(Constants.SUFFIX_SCENEINFO_PA_FPS)){
			//性能分析FPS
			OutFieldsBaseModel outputValue = new OutFieldsBaseModel();
			long i = 0;
			BigDecimal fps = new BigDecimal(0);
			String orientation = "";
			for (OutFieldsBaseModel value : values) {
				String[] array = value.getOutFields();
				fps = fps.add(StringUtil.convertBigDecimal(array[0],0));	
				orientation = array[1];
				i++;
			}
			outputValue.setOutFields(new String[]{fps.toPlainString(),String.valueOf(i),orientation});
			context.write(key, outputValue);
		}else if(key.getSuffix().equals(Constants.SUFFIX_SCENEINFO_PA_CPU)){
			//性能分析cpu
			OutFieldsBaseModel outputValue = new OutFieldsBaseModel();
			long i = 0;
			BigDecimal cpu = new BigDecimal(0);
			String orientation = "";
			for (OutFieldsBaseModel value : values) {
				String[] array = value.getOutFields();
				cpu = StringUtil.convertBigDecimal(array[0],0);
				orientation = array[1];
				i++;
			}
			//乘以1000 保留小数
			outputValue.setOutFields(new String[]{cpu.toPlainString(),String.valueOf(i),orientation});
			context.write(key, outputValue);
		}else if(key.getSuffix().equals(Constants.SUFFIX_SCENEINFO_PA_RAM)){
			//性能分析ram
			OutFieldsBaseModel outputValue = new OutFieldsBaseModel();
			long i = 0;
			BigDecimal ram = new BigDecimal(0);
			String orientation = "";
			for (OutFieldsBaseModel value : values) {
				String[] array = value.getOutFields();
				ram = ram.add(StringUtil.convertBigDecimal(array[0],0));
				orientation = array[1];
				i++;
			}
			outputValue.setOutFields(new String[]{ram.toPlainString(),String.valueOf(i),orientation});
			context.write(key, outputValue);
		}				
	}	
}
