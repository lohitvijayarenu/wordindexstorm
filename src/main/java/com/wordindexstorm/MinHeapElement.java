package com.wordindexstorm;

import java.util.Comparator;

public class MinHeapElement implements Comparator<MinHeapElement>{
	public long count;
	public String element;
	MinHeapElement(Long value, String element) {
		this.count = value;
		this.element = element;
	}

	public int compare(MinHeapElement o1, MinHeapElement o2) {
		return (int) (o1.count - o2.count);
  }
}
