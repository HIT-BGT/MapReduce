package com.bgt.graph;

public class Edge {
	private int startPoint;
	private int endPoint;
	private int weight;
	
	public Edge(){	
	}
	 public Edge (int startPoint, int endPoint, int weight ){
		 this.startPoint = startPoint;
		 this.endPoint = endPoint;
		 this.weight = weight;
	 }
	 
	 public void setStartPoint(int startPoint){
		 this.startPoint = startPoint;
	 }
	 
	 public void setEndPoint(int endPoint){
		 this.endPoint = endPoint;
	 }
	 
	 public void setWeight(int weight){
		 this.weight = weight;
	 }
	 public int getStartPoint(){
		 return this.startPoint;
	 }
	 
	 public int getEndPoint(){
		 return this.endPoint;
	 }
	 
	 public int getWeight(){
		 return this.weight;
	 }
}
