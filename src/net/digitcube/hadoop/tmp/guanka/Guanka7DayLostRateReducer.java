package net.digitcube.hadoop.tmp.guanka;

import java.io.IOException;
import java.math.BigDecimal;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.OutFieldsBaseModel;
import org.apache.hadoop.mapreduce.Reducer;

public class Guanka7DayLostRateReducer extends Reducer<OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel, OutFieldsBaseModel> {

	private OutFieldsBaseModel redValObj = new OutFieldsBaseModel();
	
	@Override
	protected void reduce(OutFieldsBaseModel key, Iterable<OutFieldsBaseModel> values, Context context) throws IOException, InterruptedException {
		int newPlayers = 0;
		int NL7Players = 0;
		for(OutFieldsBaseModel val : values){
			String newFlag = val.getOutFields()[0];
			String NL7Flag = val.getOutFields()[1];
			if("Y".equals(newFlag)){
				newPlayers++;
			}
			if(Constants.UserLostType.NewUserLost7.value.equals(NL7Flag)){
				NL7Players++;
			}
		}
		
		float rate = ((float)NL7Players/newPlayers) * 100;
		BigDecimal bd = new BigDecimal(rate + "");
		bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
		String[] keyFields = new String[]{
				bd.floatValue()+"",
				newPlayers+"",
				NL7Players+""
		};
		redValObj.setOutFields(keyFields);
		key.setSuffix("GUANKA_NL7_RATE");
		context.write(key, redValObj);
	}
	
	public static void main(String[] args){
		int a = 2;
		int b = 3;
		float value = (float)a/b;
		BigDecimal bd = new BigDecimal(value*100 + "");
		bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
		float f = bd.floatValue();
		System.out.println(f);
	}
}
