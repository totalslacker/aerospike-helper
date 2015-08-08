package com.aerospike.helper.query;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;

public class SelectorTest {
	private static final String NAMESPACE = "test";
	private static final String SET_NAME = "selector";
	private static final int RECORD_COUNT = 100;
	AerospikeClient client;
	int[] ages = new int[]{25,26,27,28,29};
	String[] colours = new String[]{"blue","red","yellow","green","orange"};
	String[] animals = new String[]{"cat","dog","mouse","snake","lion"};
	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("127.0.0.1", 3000);
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
	public void selectOnIndex() throws IOException {
		this.client.createIndex(null, NAMESPACE, SET_NAME, "age_index", "age", IndexType.NUMERIC);
		Selector selector = new Selector(client);
		Filter filter = Filter.range("age", 28, 29);
		KeyRecordIterator it = selector.query(NAMESPACE, SET_NAME, filter);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
//			System.out.println(rec);
		}
		it.close();
//		System.out.println(count);
		Assert.assertEquals(40, count);
	}
	@Test
	public void selectOnIndexWithQualifiers() throws IOException {
		this.client.createIndex(null, NAMESPACE, SET_NAME, "age_index", "age", IndexType.NUMERIC);
		Selector selector = new Selector(client);
		Filter filter = Filter.range("age", 25, 29);
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		KeyRecordIterator it = selector.query(NAMESPACE, SET_NAME, filter, qual1);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
//			System.out.println(rec);
		}
		it.close();
//		System.out.println(count);
		Assert.assertEquals(20, count);
	}
	@Test
	public void selectWithQualifiersOnly() throws IOException {
		this.client.createIndex(null, NAMESPACE, SET_NAME, "age_index", "age", IndexType.NUMERIC);
		Selector selector = new Selector(client);
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		KeyRecordIterator it = selector.query(NAMESPACE, SET_NAME, null, qual1, qual2);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
			//System.out.println(String.format("(age:%d),(color:%s)",rec.record.getValue("age"), rec.record.getValue("color")));
		}
		it.close();
		//System.out.println(count);
		Assert.assertEquals(20, count);
	}

}
