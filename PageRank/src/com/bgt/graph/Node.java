package com.bgt.graph;

import java.util.Vector;


public class Node {
	private int ID;
	private double rank;
	private Vector<Edge> adjacentEdges = new Vector<Edge>();
	public Node(String source){
		String[] result1 = source.split("\\\t");
		this.ID = Integer.valueOf((result1[0]));
		String[] result2 = result1[1].split("\\|");
		this.rank = Double.valueOf(result2[0]);
		if (result2.length > 1){
			String[] result3 = result2[1].split(",");
			for (int i = 0; i < result3.length; i++){
				Edge edge = new Edge(this.ID, Integer.valueOf(result3[i]));
				this.adjacentEdges.add(edge);
			}
		}
	}
	public Node(int ID){
		this.ID = ID;
		this.rank = 0.0;
	}
	public void  setRank(double rank){
		this.rank = rank;
	}
	public void setEdges(Vector<Edge> edges){
		this.adjacentEdges = edges;
	}
	public int getID(){
		return this.ID;
	}
	public double getRank(){
		return this.rank;
	}
	public Vector<Edge> getEdges(){
		return this.adjacentEdges;
	}
	public String getInfo(){
		String sRank = String.valueOf(this.rank);
		String sEdges = "";
		if (this.adjacentEdges.size()>0){
			for (Edge edge:this.adjacentEdges){
			   sEdges = sEdges + String.valueOf(edge.getEndPoint()) +",";
			}
			sEdges = sEdges.substring(0, sEdges.length()-1);
		}
		return String.valueOf(sRank + "|" + sEdges);
	}
}
