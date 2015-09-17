package com.aerospike.helper.query;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;

public class FlightsTest {
	private static final String NAMESPACE = "test";
	private static final String SET_NAME = "flights";
	AerospikeClient client;
	QueryEngine selector;

	@Before
	public void setUp() throws Exception {
		client = new AerospikeClient("172.28.128.6", 3000);
		selector = new QueryEngine(client);
		selector.refreshCluster();
	}

	@After
	public void tearDown() throws Exception {
		selector.close();
	}

	@Test
	public void selectNoQualifiers() throws IOException {
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("ORIGIN", "DEST", "CARRIER", "FL_NUM");
		KeyRecordIterator it = selector.select(st);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
			//System.out.println(rec);
		}
		it.close();
		//System.out.println(count);
		Assert.assertTrue(count > 900000);
	}

	@Test
	public void selectWith2Qualifiers() throws IOException {
		Qualifier qual1 = new Qualifier("ORIGIN", Qualifier.FilterOperation.EQ, Value.get("BWI"));
		Qualifier qual2 = new Qualifier("DEST", Qualifier.FilterOperation.EQ, Value.get("JFK"));
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("ORIGIN", "DEST", "CARRIER", "FL_NUM");
		KeyRecordIterator it = selector.select(st, qual1, qual2);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
//			System.out.println(rec);
		}
		it.close();
//		System.out.println(count);
		Assert.assertEquals(62, count);
	}
	@Test
	public void selectWith3Qualifiers() throws IOException {
		Qualifier qual1 = new Qualifier("ORIGIN", Qualifier.FilterOperation.EQ, Value.get("SFO"));
		Qualifier qual2 = new Qualifier("DEST", Qualifier.FilterOperation.EQ, Value.get("JFK"));
		Qualifier qual3 = new Qualifier("CARRIER", Qualifier.FilterOperation.EQ, Value.get("UA"));
		Statement st = new Statement();
		st.setNamespace(NAMESPACE);
		st.setSetName(SET_NAME);
		st.setBinNames("ORIGIN", "DEST", "CARRIER", "FL_NUM");
		KeyRecordIterator it = selector.select(st, qual1, qual2, qual3);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
			//System.out.println(rec);
		}
		it.close();
		//System.out.println(count);
		Assert.assertEquals(354, count);
	}

}
