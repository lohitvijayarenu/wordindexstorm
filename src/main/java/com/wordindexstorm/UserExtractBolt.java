package com.wordindexstorm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UserExtractBolt extends BaseBasicBolt {

  private static final long serialVersionUID = -8242125311235802843L;
  
  /**
   * process each line; extract userid as
   * first word and rest as list of words
   */
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		int i = line.indexOf(" ");
		String userId = line.substring(0, i);
		String words = line.substring(i+1);
		collector.emit(new Values(userId, words));
  }

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("userid", "words"));
  }
}
