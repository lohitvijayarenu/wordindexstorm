package com.wordindexstorm;

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
  Jedis jedis = null;
  int k = 10;
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    int boltId = context.getThisTaskId();
    int basePort = 6300;
    //int thisPort = (basePort % boltId) + boltId + 1;
    int thisPort = basePort;
    jedis = new Jedis("localhost", thisPort);    
  }

	public void execute(Tuple input, BasicOutputCollector collector) {
		String userId = input.getString(0);
		String[] words = input.getString(1).split("\\W+");
		
		for (String word : words) {
			// User userId + word
			String key = userId + "." + word;			
			Long value = jedis.incr(key);
			// Update user id and list of words
			jedis.sadd(word, userId);
			// Save topK
			updateTopK(userId+"h", word, value);
			
			// Emit <word, userid> tuple for next bolt
			collector.emit(new Values(word, userId));
		}
  }
	
	void updateTopK(String userId, String word, Long v) {
		double value = v;
		Set<redis.clients.jedis.Tuple> tuples = 
				(Set<redis.clients.jedis.Tuple>)jedis.zrangeWithScores(userId, 0, k);
		if (tuples == null || tuples.isEmpty() || tuples.size() < k) {
			jedis.zadd(userId, value, word);
		} else {
			redis.clients.jedis.Tuple t = tuples.iterator().next();
			if (t.getScore() < value) {
				jedis.zrem(userId, t.getElement());
				jedis.zadd(userId, value, word);
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "userid2"));
  }
}
