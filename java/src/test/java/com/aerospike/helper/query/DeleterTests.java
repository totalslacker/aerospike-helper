package com.aerospike.helper.query;

import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.query.Statement;

public class DeleterTests extends HelperTest{

	public DeleterTests(boolean useAuth) {
		super(useAuth);
	}
	@Test
	public void deleteByKey(){
		for (int x = 1; x <= QueryEngineTests.RECORD_COUNT; x++){
			String keyString = "selector-test:"+x;
			Key key = new Key(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, keyString);
			KeyQualifier kq = new KeyQualifier(Value.get(keyString));
			Statement stmt = new Statement();
			stmt.setNamespace(QueryEngineTests.NAMESPACE);
			stmt.setSetName(QueryEngineTests.SET_NAME);
			Map<String, Long> counts = queryEngine.delete(stmt, kq);
			Assert.assertEquals((Long)1L, (Long)counts.get("write"));
			Record record = this.client.get(null, key);
			Assert.assertNull(record);
		}
	}
	@Test
	public void deleteByDigest(){
		for (int x = 1; x <= QueryEngineTests.RECORD_COUNT; x++){
			String keyString = "selector-test:"+x;
			Key key = new Key(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, keyString);
			KeyQualifier kq = new KeyQualifier(key.digest);
			Statement stmt = new Statement();
			stmt.setNamespace(QueryEngineTests.NAMESPACE);
			stmt.setSetName(QueryEngineTests.SET_NAME);
			Map<String, Long> counts = queryEngine.delete(stmt, kq);
			Assert.assertEquals((Long)1L, (Long)counts.get("write"));
			Record record = this.client.get(null, key);
			Assert.assertNull(record);
		}
	}
	@Test
	public void deleteStartsWith() {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
		Statement stmt = new Statement();
		stmt.setNamespace(QueryEngineTests.NAMESPACE);
		stmt.setSetName(QueryEngineTests.SET_NAME);
		Map<String, Long> counts = queryEngine.delete(stmt, qual1);
		//System.out.println(counts);
		Assert.assertEquals((Long)40L, (Long)counts.get("read"));
		Assert.assertEquals((Long)40L, (Long)counts.get("write"));
		
	}
	@Test
	public void deleteEndsWith() throws IOException {
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.get("na"));
		Statement stmt = new Statement();
		stmt.setNamespace(QueryEngineTests.NAMESPACE);
		stmt.setSetName(QueryEngineTests.SET_NAME);
		Map<String, Long> counts = queryEngine.delete(stmt, qual1, qual2);
		//System.out.println(counts);
		Assert.assertEquals((Long)20L, (Long)counts.get("read"));
		Assert.assertEquals((Long)20L, (Long)counts.get("write"));
	}

}
