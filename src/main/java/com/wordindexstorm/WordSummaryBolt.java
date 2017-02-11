package com.wordindexstorm;

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
  Jedis jedis = null;
  MinHeap minHeap = null;
  int k = 10;
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
  	int boltId = context.getThisTaskId();
    int basePort = 6300;
    //int thisPort = (basePort % boltId) + boltId + 1;
    int thisPort = basePort;
    jedis = new Jedis("localhost", thisPort);   
    this.minHeap = new MinHeap(k);
  }

	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
		String userId = input.getString(1);
		
		// Update word <==> userId count
		String key = word + "." + userId;	
		Long value = jedis.incr(key);
		
		// Save topK
		updateTopK(word+"h", userId, value);
		
		// Update jedis list for each word 
		jedis.sadd(word, userId);
		
  }
	
	
	void updateTopK(String word, String userId, Long v) {
		double value = v;
		Set<redis.clients.jedis.Tuple> tuples = 
				(Set<redis.clients.jedis.Tuple>)jedis.zrangeWithScores(word, 0, k);
		if (tuples == null || tuples.isEmpty() || tuples.size() < k) {
			jedis.zadd(word, value, userId);
		} else {
			redis.clients.jedis.Tuple t = tuples.iterator().next();
			if (t.getScore() < value) {
				jedis.zrem(word, t.getElement());
				jedis.zadd(word, value, userId);
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("summary"));
  }

}
