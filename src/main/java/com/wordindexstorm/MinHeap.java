package com.wordindexstorm;

import java.util.Comparator;
import java.util.PriorityQueue;

public class MinHeap {

	public PriorityQueue<MinHeapElement> minHeap;
	int capacity;
	MinHeap(int capacity) {
		this.capacity = capacity;
		minHeap = new PriorityQueue<MinHeapElement>(capacity, 
				new Comparator<MinHeapElement>() {

					public int compare(MinHeapElement o1, MinHeapElement o2) {
	          return (int) (o1.count - o2.count);
          }
		
		});
	}
	
	boolean add(MinHeapElement element) {
		boolean added = false;
		if (minHeap.size() < capacity) {
			minHeap.add(element);
			added = true;
		} else if (minHeap.peek().count < element.count) {
			minHeap.poll(); 
			minHeap.offer(element);
			added = true;
		} 
		return added;
	}
}
