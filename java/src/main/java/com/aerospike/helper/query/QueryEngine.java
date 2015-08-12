package com.aerospike.helper.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.lua.LuaCache;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;

public class QueryEngine {
	
	private static Logger log = Logger.getLogger(QueryEngine.class);

	private AerospikeClient client;

	public QueryEngine(AerospikeClient client) {
		super();
		this.client = client;
		regusterUDF();
	}
	
	public KeyRecordIterator select(String namespace, String set, Filter filter, Qualifier... qualifiers){
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (filter != null)
			stmt.setFilters(filter);
		return select(stmt, qualifiers);
	}
	public KeyRecordIterator select(Statement stmt, Qualifier... qualifiers){
		KeyRecordIterator results = null;
		
		if (qualifiers != null && qualifiers.length > 0) {
			Map<String, Object> originArgs = new HashMap<String, Object>();
			originArgs.put("includeAllFields", 1);
			String filterFuncStr = buildFilterFunction(qualifiers);
			originArgs.put("filterFuncStr", filterFuncStr);
			stmt.setAggregateFunction(this.getClass().getClassLoader(), "com/aerospike/helper/query/as_utility.lua", "as_utility", "select_records", Value.get(originArgs));
			ResultSet resultSet = this.client.queryAggregate(null, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), resultSet);
		} else {
			RecordSet recordSet = this.client.query(null, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), recordSet);
		} 
		return results;
	}
	
	private String buildFilterFunction(Qualifier[] qualifiers) {
		StringBuilder sb = new StringBuilder("if ");
		for (int i = 0; i < qualifiers.length; i++){
			
			sb.append(qualifiers[i].luaFilterString());
			if (qualifiers.length > 1 && i < (qualifiers.length -1) )
				sb.append(" and ");
		}
		sb.append(" then selectedRec = true end");
		return sb.toString();
	}
	

	private void regusterUDF() {
		Node[] nodes = this.client.getNodes();
		String moduleString = Info.request(nodes[0], "udf-list");
		if (moduleString.isEmpty()
				|| !moduleString.contains("as_utility.lua")){ // register the udf module

			this.client.register(null, this.getClass().getClassLoader(), 
					"com/aerospike/helper/query/as_utility.lua", 
					"as_utility.lua", Language.LUA);
			
		}
	}

}
