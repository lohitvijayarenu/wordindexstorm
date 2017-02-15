package com.wordindexstorm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.PriorityQueue;
import javax.xml.bind.DatatypeConverter;

public class MinHeap implements Serializable {

  private static final long serialVersionUID = -9186648355313626590L;
  
	public PriorityQueue<MinHeapElement> minHeap;
	MinHeap() {
		minHeap = new PriorityQueue<MinHeapElement>();
	}
	
	boolean add(MinHeapElement element, int capacity) {
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
	
	String serialize() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    
    oos.writeObject( minHeap );
    oos.close();
    return DatatypeConverter.printBase64Binary(baos.toByteArray());
	}
	
	void deserialize(String s) throws IOException, ClassNotFoundException {
		byte [] data = DatatypeConverter.parseBase64Binary(s);
    ObjectInputStream ois = new ObjectInputStream( 
                                    new ByteArrayInputStream(data));
    minHeap  = 
    		(PriorityQueue<MinHeapElement>) ois.readObject();
    ois.close();
	}
}
