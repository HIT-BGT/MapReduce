package com.bgt.skiplist;

public class Node {
	private int data;
	private Node downNode;
	private Node nextNode;
	
	public Node(){
	}
	
	public Node(int data){
		this.data = data;
		this.downNode = null;
		this.nextNode  = null;
	}
	
	public Node(int data, Node downNode, Node nextNode){
		this.data = data;
		this.downNode = downNode;
		this.nextNode = nextNode;
	}
	
	public void setData(int data){
		this.data = data;
	}
	
	public void setDownNode(Node downNode){
		this.downNode = downNode;
	}
	
	public void setNextNode(Node nextNode){
		this.nextNode = nextNode;
	}
	
	public int getData(){
		return this.data;
	}
	
	public Node getDownNode(){
		return this.downNode;
	}
	
	public Node getNextNode(){
		return this.nextNode;
	}
}
