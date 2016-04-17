package com.aerospike.helper.collections;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.helper.query.TestQueryEngine;

public class CollectionsTimeSeries {
	
	public static final String NAMESPACE = "test";
	public static final String SET = "time_series";
	
	protected static Logger log = Logger.getLogger(CollectionsTimeSeries.class);
	private AerospikeClient client;

	@Before
	public void setUp() {
		try
		{
			log.info("Creating AerospikeClient");
			ClientPolicy clientPolicy = new ClientPolicy();
			clientPolicy.timeout = TestQueryEngine.TIME_OUT;
			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
			client.writePolicyDefault.expiration = 1800;

		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	@After
	public void tearDown() {
		log.info("Closing AerospikeClient");
		client.close();
	}
	
	@Test
	public void add100IntOneByOne(){
		Key key = new Key (NAMESPACE, SET, "100-time-series-key-int");
		client.delete(null, key);
		TimeSeries t = new TimeSeries (client, null, key, "100-ts-int");
		for (int x = 0; x < 100; x++){
			long timeStamp = System.currentTimeMillis();
			t.add(timeStamp, Value.get(x));
		}
		t.findAll();
		Assert.assertEquals (100, ll.size ());
		t.destroy ();
		client.delete(null, key);
	}


}
