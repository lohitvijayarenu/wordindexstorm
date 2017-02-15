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
  JedisConnectionCache jedisConnections = null;
  int k = 10;
  long count = 0;
  
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
  	int boltId = context.getThisTaskIndex();
    System.out.println("WordSummaryBolt " + boltId);
    jedisConnections = new JedisConnectionCache();
  }

	public void execute(Tuple input, BasicOutputCollector collector) {
		String userId = input.getString(0);
		String[] words = input.getString(1).split("\\W+");
		count++;
		if ((count % 100000) == 0) {
			System.out.println("UserSummaryBolt processed " + count);
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
		double value = v;
		Jedis jedis = jedisConnections.getJedisConnection(userId);
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
