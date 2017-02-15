package com.wordindexstorm;

import redis.clients.jedis.Jedis;

public class JedisConnectionCache {
	Jedis[] jedisConnections;
	int numConnections = 16;
	JedisConnectionCache() {
		// Hardcode for now
		String[] hosts = new String[]{"ip-10-0-0-30", "ip-10-0-0-31", 
				"ip-10-0-0-32", "ip-10-0-0-33"};
		int[] ports = new int[]{6300, 6301, 6302, 6303};
		
		jedisConnections = new Jedis[16];
		
		int i=0;
    for (String host : hosts) {
    	for (int port : ports) {
    		jedisConnections[i++] = new Jedis(host, port);
    		System.out.println("Creating jedis connection to : " + host + ":" + port);
    	}
    }
	}
	
	public Jedis getJedisConnection(String key) {
		long hash = 7;
		for (int i=0; i < key.length(); i++)
			hash = hash*31 + key.charAt(i);
		
		return jedisConnections[(int) (Math.abs(hash) % numConnections)];
	}
	
	public int getNumConnections() {
		return numConnections;
	}

}
