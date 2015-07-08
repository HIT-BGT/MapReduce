package com.bgt.skiplist;

public class SkipList {
	public Node firstNode = new Node(-1);
	public Node lastNode = new Node(1);
	public int height;
	public Node createSkipList(String Info){
		Node preNode = firstNode;
		String[] result = Info.split(" ");
		for (int i=0;i<result.length;i++){
			Node node = new Node(Integer.valueOf(result[i]));
			preNode.setNextNode(node);
			preNode = preNode.getNextNode();
		}
		preNode.setNextNode(lastNode);
		
	}
}
