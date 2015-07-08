package com.bgt.graph;

import java.util.Vector;


public class Node {
	private int ID;
	private int status;	//0 = Not Considered; 1 = Considering; 2 = Considered
	private int distance;
	private Vector<Edge> adjacentEdges = new Vector<Edge>();
	public Node(String source){
		String[] result1 = source.split("\\\t");
		this.ID = Integer.valueOf((result1[0]));
		String[] result2 = result1[1].split("\\|");
		this.status = Integer.valueOf(result2[0]);
		if ("Infinity" .equals(result2[1])){
			this.distance = Integer.MAX_VALUE;;
		}else{
			this.distance = Integer.valueOf(result2[1]);
		}
		if (result2.length > 2){
			String[] result3 = result2[2].split(",");
			for (int i = 0; i < result3.length; i+=2){
				Edge edge = new Edge(this.ID, Integer.valueOf(result3[i]), Integer.valueOf(result3[i+1]));
				this.adjacentEdges.add(edge);
			}
		}
	}
	public Node(int ID){
		this.ID = ID;
		this.status = 0;
		this.distance = Integer.MAX_VALUE;
	}
	public void setStatus(int status){
		this.status = status;
	}
	public void  setDistance(int distance){
		this.distance = distance;
	}
	public void setEdges(Vector<Edge> edges){
		this.adjacentEdges = edges;
	}
	public int getID(){
		return this.ID;
	}
	public int getStatus(){
		return this.status;
	}
	public int getDistance(){
		return this.distance;
	}
	public Vector<Edge> getEdges(){
		return this.adjacentEdges;
	}
	public String getInfo(){
		String sDistance = null;
		String sEdges = "";
		if (this.distance == Integer.MAX_VALUE){
			sDistance = "Infinity";
		}else{
			sDistance = String.valueOf(this.distance);
		}
		if (this.adjacentEdges.size()>0){
			for (Edge edge:this.adjacentEdges){
			   sEdges = sEdges + String.valueOf(edge.getEndPoint()) + "," + String.valueOf(edge.getWeight())+",";
			}
			sEdges = sEdges.substring(0, sEdges.length()-1);
		}
		return String.valueOf(this.status) + "|" + sDistance + "|" + sEdges;
	}
}
