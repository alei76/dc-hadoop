package net.digitcube.hadoop.mapreduce.gameexperience.po;

import net.digitcube.hadoop.common.Constants;
import net.digitcube.hadoop.common.MRConstants;
import net.digitcube.hadoop.util.StringUtil;

public class Point {
	
	private String x;	
	private String y;
	private String orientation;
	private String resolution;	
	//固定320x480
	private int fixedX = Constants.SCENEINFO_RESOLUTION_X;	
	private int fixedY = Constants.SCENEINFO_RESOLUTION_Y;
	//格子固定10x10
	private int checkX = 10;
	private int checkY = 10;	
	
	public Point(){	}
	
	public Point(String x,String y,String resolution){
		this.x = x;
		this.y = y;
		this.resolution = resolution;
	}
	
	public Point(String[] xy){
		this.x = xy[0];
		this.y = xy[1];
	}
	
	public Point(String[] xy,String resolution){
		this.x = xy[0];
		this.y = xy[1];
		this.resolution = resolution;
	}
	
	public String getX() {
		return x;
	}
	
	public void setX(String x) {
		this.x = x;
	}
	
	public String getY() {
		return y;
	}
	
	public void setY(String y) {
		this.y = y;
	}
	
	public int getFixedX() {
		return fixedX;
	}

	public void setFixedX(int fixedX) {
		this.fixedX = fixedX;
	}

	public int getFixedY() {
		return fixedY;
	}

	public void setFixedY(int fixedY) {
		this.fixedY = fixedY;
	}

	public int getCheckX() {
		return checkX;
	}

	public void setCheckX(int checkX) {
		this.checkX = checkX;
	}

	public void setCheckY(int checkY) {
		this.checkY = checkY;
	}	
	
	public int getCheckY() {
		return checkY;
	}
	
	public String getOrientation() {
		return orientation;
	}
	
	public void setOrientation(String orientation) {
		this.orientation = orientation;
	}
	
	public String getResolution() {
		return resolution;
	}

	public void setResolution(String resolution) {
		this.resolution = resolution;
	}

	//转换成固定分辨率的格子中心点
	public Point transPoint(String orientation,String resolution){		
		String[] ra = resolution.split(MRConstants.SEPERATOR_SCENE_RESOLUTION);		
		int resolutionX = StringUtil.convertInt(ra[0], 0);
		int resolutionY = StringUtil.convertInt(ra[1], 0);	
		return transCheckPoint(orientation,transX(orientation,resolutionX,resolutionY),transY(orientation,resolutionX,resolutionY));
	}
	
	//转换格子中心点  每个格子为10*10
	public Point transCheckPoint(String orientation,int transedX,int transedY){
//		System.out.println(orientation + ":" + transedX + ":" + transedY);
		
		int x = (transedX/checkX + 1)*checkX - (checkX/2);		
		int y = (transedY/checkY + 1)*checkY - (checkY/2);
		if(Constants.SCENEINFO_ORIENTATION_PORTRAIT.equals(orientation)){			
			//如果是竖屏  处理原点转换
			//y = fixedY - y;
			//如果是竖屏  处理越界
			if(x > fixedX){
				x = fixedX - (checkX/2);
			}
			if(y > fixedY){
				y = fixedY - (checkY/2);
			}
			if(y < 0){
				y = (checkY/2);
			}
		}
		if(Constants.SCENEINFO_ORIENTATION_LANDSCAPE.equals(orientation)){
			//如果是横屏 处理原点转换		
			//y = fixedX - y;
			//如果是横屏 处理越界			
			if(x > fixedY){
				x = fixedY - (checkY/2);
			}
			if(y > fixedX){
				y = fixedX - (checkY/2);
			}
			if(y < 0){
				y = (checkY/2);
			}
		}
		
		
		return new Point(String.valueOf(x),String.valueOf(y),orientation);		
	}	
	
	public int transX(String orientation,int resolutionX,int resolutionY){		
		if(Constants.SCENEINFO_ORIENTATION_PORTRAIT.equals(orientation)){
			//如果是竖屏
			return (fixedX * StringUtil.convertInt(x,0))/resolutionX;
		}else if(Constants.SCENEINFO_ORIENTATION_LANDSCAPE.equals(orientation)){
			//如果是横屏
			return (fixedY * StringUtil.convertInt(x,0))/resolutionY;
		}else{
			return 0;
		}		
	}
	
	private int transY(String orientation,int resolutionX,int resolutionY){		
		if(Constants.SCENEINFO_ORIENTATION_PORTRAIT.equals(orientation)){
			//如果是竖屏
			return (fixedY * StringUtil.convertInt(y,0))/resolutionY;
		}else if(Constants.SCENEINFO_ORIENTATION_LANDSCAPE.equals(orientation)){
			//如果是横屏
			return (fixedX * StringUtil.convertInt(y,0))/resolutionX;
		}else{
			return 0;
		}				
	}
	
	public static void main(String[] args) {
		Point point = new Point(new String[]{"0","0"},Constants.SCENEINFO_ORIENTATION_PORTRAIT);
		point = point.transPoint(Constants.SCENEINFO_ORIENTATION_PORTRAIT, "500x700");
		System.out.println(point.getX());
		System.out.println(point.getY());
		
		Point point1 = new Point(new String[]{"0","0"},Constants.SCENEINFO_ORIENTATION_LANDSCAPE);
		point1 = point1.transPoint(Constants.SCENEINFO_ORIENTATION_LANDSCAPE, "500x700");
		System.out.println(point1.getX());
		System.out.println(point1.getY());
		
		
//		Point point = new Point(new String[]{"500","0"},Constants.SCENEINFO_ORIENTATION_PORTRAIT);
//		point = point.transPoint(Constants.SCENEINFO_ORIENTATION_PORTRAIT, "500x700");
//		System.out.println(point.getX());
//		System.out.println(point.getY());
//		
//		Point point1 = new Point(new String[]{"700","0"},Constants.SCENEINFO_ORIENTATION_LANDSCAPE);
//		point1 = point1.transPoint(Constants.SCENEINFO_ORIENTATION_LANDSCAPE, "500x700");
//		System.out.println(point1.getX());
//		System.out.println(point1.getY());
		
		
//		Point point = new Point(new String[]{"0","700"},Constants.SCENEINFO_ORIENTATION_PORTRAIT);
//		point = point.transPoint(Constants.SCENEINFO_ORIENTATION_PORTRAIT, "500x700");
//		System.out.println(point.getX());
//		System.out.println(point.getY());
//		
//		Point point1 = new Point(new String[]{"0","500"},Constants.SCENEINFO_ORIENTATION_LANDSCAPE);
//		point1 = point1.transPoint(Constants.SCENEINFO_ORIENTATION_LANDSCAPE, "500x700");
//		System.out.println(point1.getX());
//		System.out.println(point1.getY());
		
		
//		Point point = new Point(new String[]{"500","700"},Constants.SCENEINFO_ORIENTATION_PORTRAIT);
//		point = point.transPoint(Constants.SCENEINFO_ORIENTATION_PORTRAIT, "500x700");
//		System.out.println(point.getX());
//		System.out.println(point.getY());
//		
//		Point point1 = new Point(new String[]{"700","500"},Constants.SCENEINFO_ORIENTATION_LANDSCAPE);
//		point1 = point1.transPoint(Constants.SCENEINFO_ORIENTATION_LANDSCAPE, "500x700");
//		System.out.println(point1.getX());
//		System.out.println(point1.getY());
		
		
		
		
	}
}
