package com.wordindexstorm;


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordIndexStorm {
	
	public static void main(String[] args) 
			throws AlreadyAliveException, InvalidTopologyException {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RandomInputGeneratorSpout(), 4);
		builder.setBolt("userextract", new UserExtractBolt(), 4)
			.shuffleGrouping("spout");
		builder.setBolt("usersummary", new UserSummaryBolt(), 4)
		  .setNumTasks(4)
    	.fieldsGrouping("userextract", new Fields("userid"));
		builder.setBolt("wordsummary", new WordSummaryBolt(), 4)
			.setNumTasks(4)
			.fieldsGrouping("usersummary", new Fields("word"));
  
		Config conf = new Config();
		conf.setNumWorkers(16);

		StormSubmitter.submitTopology("wordindex", conf, 
				builder.createTopology());
	}	
}
