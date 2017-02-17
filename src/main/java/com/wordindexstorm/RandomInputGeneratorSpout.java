package com.wordindexstorm;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RandomInputGeneratorSpout extends BaseRichSpout {

  private static final long serialVersionUID = -4114951263153715761L;
	SpoutOutputCollector collector;
	int maxUsers = 100000000;
	int maxWordsPerStmt = 10;
	int wordLength = 17;
	long startTime;
  int count = 0;
  int EVENTS_PER_SECOND = 10000;
  int eventsPerSecond = EVENTS_PER_SECOND;
  long currStartTime = System.currentTimeMillis();
  long logStartTime = currStartTime;
  long timeInterval = 10000;
  long period = 1000;
  

	public void nextTuple() {
		eventsPerSecond--; count++;
		if (eventsPerSecond < 0) {
			long now = System.currentTimeMillis();
			if ((now - logStartTime) > timeInterval) {
				logStartTime = now;
				System.out.println("Time RandomInputGenerator processed " + count);
			}
			long currEndTime = currStartTime + period;
			if (now < currEndTime) {
				try {
	        Thread.sleep(currEndTime - now);
        } catch (InterruptedException e) {
        }
			}
			eventsPerSecond = EVENTS_PER_SECOND;
			currStartTime = System.currentTimeMillis();
		}
		String line = getLine();
		collector.emit(new Values(line));
  }

	/*
	 * Use same logic as wordindex for dynox to generate input
	 */
	private String getLine() {
		long userId = 0 + (long)(Math.random() * maxUsers);	
		int length = 1 + (int)(Math.random() * maxWordsPerStmt);
    StringBuilder sb = new StringBuilder();
    // Add first word as userId
    sb.append(String.valueOf(userId)).append(' ');
    
    
    // Add rest of words at random.
    for (int i = 0; i < length - 1; i++) {
      sb.append(generateWord(userId));
      sb.append(' ');
    }
    sb.append(generateWord(userId));
    return (sb.toString());
  }
	
	private String generateWord(long userid) {
    StringBuilder sb = new StringBuilder();
    double probA = (userid % 2 == 0) ? 0.7 : 0.3;
    for (int i = 0; i < wordLength; i++) {
      sb.append(Math.random() < probA ? 'A' : 'B');
    }
    return (sb.toString());
  }

	public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
	  this.collector = collector;
  }

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
  }

}
