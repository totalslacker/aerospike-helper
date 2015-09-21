package com.aerospike.helper.query;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;

public class SelectorTest extends HelperTest{



	public SelectorTest(boolean useAuth) {
		super(useAuth);
	}
	
	@Test
	public void selectOne() throws IOException {
		Statement stmt = new Statement();
		stmt.setNamespace(QueryEngineTests.NAMESPACE);
		stmt.setSetName(QueryEngineTests.SET_NAME);
		KeyQualifier kq = new KeyQualifier(Value.get("selector-test:3"));
		KeyRecordIterator it = queryEngine.select(stmt, kq);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
//			System.out.println(rec);
		}
		it.close();
//		System.out.println(count);
		Assert.assertEquals(1, count);
	}

	@Test
	public void selectAll() throws IOException {
		KeyRecordIterator it = queryEngine.select(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, null);
		int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			count++;
//			System.out.println(rec);
		}
		it.close();
//		System.out.println(count);
		Assert.assertEquals(QueryEngineTests.RECORD_COUNT, count);
	}

	@Test
	public void selectOnIndex() throws IOException {
		this.client.createIndex(null, QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		Filter filter = Filter.range("age", 28, 29);
		KeyRecordIterator it = queryEngine.select(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, filter);
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
	public void selectStartsWith() throws IOException {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
		KeyRecordIterator it = queryEngine.select(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, null, qual1);
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
	public void selectEndsWith() throws IOException {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.get("na"));
		KeyRecordIterator it = queryEngine.select(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, null, qual1, qual2);
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
	public void selectOnIndexWithQualifiers() throws IOException {
		this.client.createIndex(null, QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		Filter filter = Filter.range("age", 25, 29);
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		KeyRecordIterator it = queryEngine.select(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, filter, qual1);
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
		this.client.createIndex(null, QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, "age_index", "age", IndexType.NUMERIC);
		queryEngine.refreshCluster();
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("green"));
		Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.get(28), Value.get(29));
		KeyRecordIterator it = queryEngine.select(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, null, qual1, qual2);
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
	@Test
	public void selectWithGeneration() throws IOException {
		queryEngine.refreshCluster();
		Qualifier qual1 = new GenerationQualifier(Qualifier.FilterOperation.GTEQ, Value.get(1));
		KeyRecordIterator it = queryEngine.select(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, null, qual1);
		//int count = 0;
		while (it.hasNext()){
			KeyRecord rec = it.next();
			Assert.assertTrue(rec.record.generation >= 1);
			//count++;
			//System.out.println(String.format("(age:%d),(color:%s)",rec.record.getValue("age"), rec.record.getValue("color")));
		}
		it.close();
		//System.out.println(count);
	}

}
