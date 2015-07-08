package com.bgt.mycomparator;

import java.util.Comparator;

public class MyComparator implements Comparator<Compound>{
	@Override
	public int compare(Compound c1, Compound c2) {
		if (c1.count == c2.count) return 0;
		else return c1.count>c2.count?1:-1;
	}
}
