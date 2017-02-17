package com.wordindexstorm;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UserSummaryBolt extends BaseBasicBolt {

  private static final long serialVersionUID = 6157289959700753098L;
  JedisConnectionCache jedisConnections = null;
  int k = 10;
  long count = 0;
  long startTime = System.currentTimeMillis();
  long interval = 10000;
  MinHeap minHeap = null;
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
  	int boltId = context.getThisTaskIndex();
    System.out.println("WordSummaryBolt " + boltId);
    jedisConnections = new JedisConnectionCache();
    minHeap = new MinHeap();
  }

	public void execute(Tuple input, BasicOutputCollector collector) {
		String userId = input.getString(0);
		String[] words = input.getString(1).split("\\W+");
		count++;
		if ((count % 100000) == 0) {
			System.out.println("UserSummaryBolt processed " + count);
		}
		long now = System.currentTimeMillis();
		if ((now - startTime) > interval) {
			startTime = now;
			System.out.println("Time UserSummaryBolt processed " + count);
		}
		
		for (String word : words) {
			// User userId + word
			String key = userId + "." + word;			
			Long value = jedisConnections.getJedisConnection(key).incr(key);
			// Update user id and list of words
			jedisConnections.getJedisConnection(word).sadd(word, userId);
			// Save topK
			updateTopK(userId+"h", word, value);
			
			// Emit <word, userid> tuple for next bolt
			collector.emit(new Values(word, userId));
		}
  }
	
	void updateTopK(String userId, String word, Long v) {
		Jedis jedis = jedisConnections.getJedisConnection(userId);
		minHeap = new MinHeap();
		String topKBlob = jedis.get(userId);
		try {
			if(topKBlob != null) {
				minHeap.deserialize(topKBlob);
			} 
			minHeap.add(new MinHeapElement(v, word), k);
			topKBlob = minHeap.serialize();
			jedis.set(userId, topKBlob);
			
    } catch (ClassNotFoundException e) {
	    e.printStackTrace();
    } catch (IOException e) {
	    e.printStackTrace();
    }
		minHeap = null;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "userid2"));
  }
}
