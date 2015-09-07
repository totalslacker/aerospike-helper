package com.aerospike.helper.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;

public class UpdatorTest {
	private static final String NAMESPACE = "test";
	private static final String SET_NAME = "selector";
	private static final int RECORD_COUNT = 100;
	AerospikeClient client;
	int[] ages = new int[]{25,26,27,28,29};
	String[] colours = new String[]{"blue","red","yellow","green","orange"};
	String[] animals = new String[]{"cat","dog","mouse","snake","lion"};

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("172.28.128.6", 3000);
		int i = 0;
		for (int x = 1; x <= RECORD_COUNT; x++){
			Key key = new Key(NAMESPACE, SET_NAME, "selector-test:"+ x);
			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			this.client.put(null, key, name, age, colour, animal);
			i++;
			if ( i == 5)
				i = 0;
		}
	}

	@After
	public void tearDown() throws Exception {
		for (int x = 1; x <= RECORD_COUNT; x++){
			Key key = new Key(NAMESPACE, SET_NAME, "selector-test:"+ x);
			this.client.delete(null, key);
		}
		client.close();
	}

	@Test
	public void updateStartsWith() {
		QueryEngine updator = new QueryEngine(client);
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
		ArrayList<Bin> bins = new ArrayList<Bin>() {{
		    add(new Bin("ending", "ends with e"));
		}};
		Map<String, Long> counts = updator.update(NAMESPACE, SET_NAME, bins, null, qual1);
		System.out.println(counts);
		Assert.assertEquals((Long)40L, (Long)counts.get("read"));
		Assert.assertEquals((Long)40L, (Long)counts.get("write"));
	}
	@Test
	public void updateEndsWith() throws IOException {
		QueryEngine updator = new QueryEngine(client);
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.get("na"));
		ArrayList<Bin> bins = new ArrayList<Bin>() {{
		    add(new Bin("starting", "ends with e"));
		}};
		Map<String, Long> counts = updator.update(NAMESPACE, SET_NAME, bins, null, qual1, qual2);
		System.out.println(counts);
		Assert.assertEquals((Long)20L, (Long)counts.get("read"));
		Assert.assertEquals((Long)20L, (Long)counts.get("write"));
	}

}
