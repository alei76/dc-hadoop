package net.digitcube.hadoop.util;

public class PlayerType {

	private int playerType;
	private static final int PLAYER_TYPE_NEW_ADD = 1;
	private static final int PLAYER_TYPE_ONLINE = 2;
	private static final int PLAYER_TYPE_PAY = 4;
	private static final int PLAYER_TYPE_EVER_PAY = 8;
	
	public void markNewAdd(){
		playerType = playerType | PLAYER_TYPE_NEW_ADD;
	}
	public void markOnline(){
		playerType = playerType | PLAYER_TYPE_ONLINE;
	}
	public void markPay(){
		playerType = playerType | PLAYER_TYPE_PAY;
	}
	public void markEverPay(){
		playerType = playerType | PLAYER_TYPE_EVER_PAY;
	}
	
	public boolean isNewAdd(){
		return (playerType & PLAYER_TYPE_NEW_ADD) > 0;
	}
	public boolean isOnline(){
		return (playerType & PLAYER_TYPE_ONLINE) > 0;
	}
	public boolean isPay(){
		return (playerType & PLAYER_TYPE_PAY) > 0;
	}
	public boolean isEverPay(){
		return (playerType & PLAYER_TYPE_EVER_PAY) > 0;
	}
	public void reset(){
		this.playerType = 0;
	}
	
	public PlayerType() {
	}
	public PlayerType(int playerType) {
		this.playerType = playerType;
	}
	
	@Override
	public String toString() {
		return playerType+"";
		/*return "PlayerType [playerType=" + playerType + ", "
				+ "isNewAdd="+isNewAdd() + ", "
				+ "isOnline="+isOnline() + ", "
				+ "isPay="+isPay() + ", "
				+ "isEverPay="+isEverPay() + "]\n";*/
	}
	public static void main(String[] args){
		PlayerType p = new PlayerType();
		p.markNewAdd();
		p.markOnline();
		p.markPay();
		p.markEverPay();
		System.out.println(p);
		System.out.println(p.playerType + "=" + Integer.toBinaryString(p.playerType));
	}
}
