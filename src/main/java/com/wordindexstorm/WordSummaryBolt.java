package com.wordindexstorm;

import java.io.IOException;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WordSummaryBolt extends BaseBasicBolt{

  private static final long serialVersionUID = 638113626345284876L;
  JedisConnectionCache jedisConnections = null;
  int k = 10;
  long count = 0;
  MinHeap minHeap;
  long startTime = System.currentTimeMillis();
  long interval = 10000;
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
  	int boltId = context.getThisTaskIndex();
    System.out.println("WordSummaryBolt " + boltId);
    jedisConnections = new JedisConnectionCache();
    minHeap = new MinHeap();
  }

	public void execute(Tuple input, BasicOutputCollector collector) {
		count++;
		if ((count % 100000) == 0) {
			System.out.println("WordSummaryBolt processed " + count);
		}
		long now = System.currentTimeMillis();
		if ((now - startTime) > interval) {
			startTime = now;
			System.out.println("Time WordSummaryBolt processed " + count);
		}
		
		String word = input.getString(0);
		String userId = input.getString(1);
		
		// Update word <==> userId count
		String key = word + "." + userId;
		Long value = jedisConnections.getJedisConnection(key).incr(key);	
		// Save topK
		updateTopK(word+"h", userId, value);
		
		// Update jedis list for each word 
		jedisConnections.getJedisConnection(word).sadd(word, userId);		
  }
	
	void updateTopK(String word, String userId, Long v)  {
		Jedis jedis = jedisConnections.getJedisConnection(word);
		minHeap = new MinHeap();
		String topKBlob = jedis.get(word);
		try {
			if(topKBlob != null) {
				minHeap.deserialize(topKBlob);
			} 
			minHeap.add(new MinHeapElement(v, userId), k);
			topKBlob = minHeap.serialize();
			jedis.set(word, topKBlob);
			
    } catch (ClassNotFoundException e) {
	    e.printStackTrace();
    } catch (IOException e) {
	    e.printStackTrace();
    }
		minHeap = null;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("summary"));
  }

}
