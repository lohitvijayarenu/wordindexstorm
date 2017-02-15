package com.wordindexstorm;

import java.io.Serializable;

public class MinHeapElement implements Serializable, Comparable<MinHeapElement>{
	
  private static final long serialVersionUID = 5790756323215886666L;
	public Long count;
	public String element;
	MinHeapElement(Long value, String element) {
		this.count = value;
		this.element = element;
	}
	public int compareTo(MinHeapElement o) {
    return (int) (count - o.count);
  }
}
