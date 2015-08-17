package net.digitcube.hadoop.common;

public class AdLabel {

	public final static int PayAbility_Amount_High = 1;
	public final static int PayAbility_Amount_Normal = 2;
	public final static int PayAbility_Amount_Low = 3;
	public final static int PayAbility_Frequency_High = 1;
	public final static int PayAbility_Frequency_Normal = 2;
	public final static int PayAbility_Frequency_Low = 3;
	public final static int PayAbility_Habit_High = 1;
	public final static int PayAbility_Habit_Normal = 2;

	private int pa_amt;
	private int pa_fre;
	private int pa_habit;
	private String ap_ga;
	private String lv_ht;

	public int getPa_amt() {
		return pa_amt;
	}

	public void setPa_amt(int pa_amt) {
		this.pa_amt = pa_amt;
	}

	public int getPa_fre() {
		return pa_fre;
	}

	public void setPa_fre(int pa_fre) {
		this.pa_fre = pa_fre;
	}

	public int getPa_habit() {
		return pa_habit;
	}

	public void setPa_habit(int pa_habit) {
		this.pa_habit = pa_habit;
	}

	public String getAp_ga() {
		return ap_ga;
	}

	public void setAp_ga(String ap_ga) {
		this.ap_ga = ap_ga;
	}

	public String getLv_ht() {
		return lv_ht;
	}

	public void setLv_ht(String lv_ht) {
		this.lv_ht = lv_ht;
	}
}
