package com.aerospike.helper.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.query.Statement;

public class DeleterTests extends HelperTest{

	public DeleterTests(boolean useAuth) {
		super(useAuth);
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
