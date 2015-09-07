package com.aerospike.helper.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
/**
 * This class provides a multi-filter query engine that
 * augments the query capability in Aerospike.
 * To achieve this the class uses a UserDefined Function written in Lua to 
 * provide the additional filtering. This UDF module packaged in the JAR and is automatically registered
 * with the cluster.
 * @author peter
 *
 */
public class QueryEngine {
	
	private static Logger log = Logger.getLogger(QueryEngine.class);

	private AerospikeClient client;
	public WritePolicy updatePolicy;
	/**
	 * The Query engine is constructed by passing in an existing 
	 * AerospikeClient instance
	 * @param client
	 */
	public QueryEngine(AerospikeClient client) {
		super();
		this.client = client;
		registerUDF();
		this.updatePolicy = new WritePolicy(this.client.writePolicyDefault);
		this.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
	}
	
	/*
	 * *****************************************************
	 * 
	 * Select
	 * 
	 * ***************************************************** 
	 */
	
	
	/**
	 * @param namespace
	 * @param setName
	 * @param filter
	 * @param sortMap
	 * @param qualifiers
	 * @return
	 */
	public KeyRecordIterator select(String namespace, String set, Filter filter, Map<String, String> sortMap, Qualifier... qualifiers){
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (filter != null)
			stmt.setFilters(filter);
		return select(stmt, sortMap, qualifiers);	

	}
	public KeyRecordIterator select(Statement stmt, Map<String, String> sortMap, Qualifier... qualifiers){
		KeyRecordIterator results = null;
		
		if (qualifiers != null && qualifiers.length > 0) {
			Map<String, Object> originArgs = new HashMap<String, Object>();
			originArgs.put("includeAllFields", 1);
			String filterFuncStr = buildFilterFunction(qualifiers);
			originArgs.put("filterFuncStr", filterFuncStr);
			String sortFuncStr = buildSortFunction(sortMap);
			originArgs.put("sortFuncStr", sortFuncStr);
			stmt.setAggregateFunction(this.getClass().getClassLoader(), "com/aerospike/helper/query/as_utility.lua", "as_utility", "select_records", Value.get(originArgs));
			ResultSet resultSet = this.client.queryAggregate(null, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), resultSet);
		} else {
			RecordSet recordSet = this.client.query(null, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), recordSet);
		} 
		return results;
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
	
	/*
	 * *****************************************************
	 * 
	 * Update
	 * 
	 * ***************************************************** 
	 */
	public Map<String, Long> update(String namespace, String set, List<Bin> bins, Filter filter, Qualifier... qualifiers){
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (filter != null)
			stmt.setFilters(filter);
		return update(stmt, bins, qualifiers);	

	}
	public Map<String, Long> update(Statement stmt, List<Bin> bins, Qualifier... qualifiers){
		KeyRecordIterator results = null;
		
		if (qualifiers != null && qualifiers.length > 0) {
			Map<String, Object> originArgs = new HashMap<String, Object>();
			originArgs.put("includeAllFields", 1);
			String filterFuncStr = buildFilterFunction(qualifiers);
			originArgs.put("filterFuncStr", filterFuncStr);
			stmt.setAggregateFunction(this.getClass().getClassLoader(), "com/aerospike/helper/query/as_utility.lua", "as_utility", "query_meta", Value.get(originArgs));
			ResultSet resultSet = this.client.queryAggregate(null, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), resultSet);
		} else {
			RecordSet recordSet = this.client.query(null, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), recordSet);
		} 
		return update(results, bins);
	}
	
	private Map<String, Long> update(KeyRecordIterator results, List<Bin> bins){
		long readCount = 0;
		long updateCount = 0;
		while (results.hasNext()){
			KeyRecord keyRecord = results.next();
			readCount++;
			updatePolicy.generation = keyRecord.record.generation;
			try {
				client.put(updatePolicy, keyRecord.key, bins.toArray(new Bin[0]));
				updateCount++;
			} catch (AerospikeException e){
				System.out.println(keyRecord.key);
			}
		}
		Map<String, Long> map = new HashMap<String, Long>();
		map.put("read", readCount);
		map.put("write", updateCount);
		return map;
	}
	
	
	
	private String buildSortFunction(Map<String, String> sortMap) {
		// TODO Auto-generated method stub
		return null;
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
	

	private void registerUDF() {
		Node[] nodes = this.client.getNodes();
		String moduleString = Info.request(nodes[0], "udf-list");
		if (moduleString.isEmpty()
				|| !moduleString.contains("as_utility.lua")){ // register the spring_api udf module

			this.client.register(null, this.getClass().getClassLoader(), 
					"com/aerospike/helper/query/as_utility.lua", 
					"as_utility.lua", Language.LUA);
			
		}
	}

}
