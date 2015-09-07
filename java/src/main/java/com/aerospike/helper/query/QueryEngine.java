package com.aerospike.helper.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.helper.model.Index;
import com.aerospike.helper.model.Module;
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
	private Map<String, Index> indexCache;
	public WritePolicy updatePolicy;
	public WritePolicy insertPolicy;
	private InfoPolicy infoPolicy;

	private Map<String, Module> moduleCache;
	/**
	 * The Query engine is constructed by passing in an existing 
	 * AerospikeClient instance
	 * @param client
	 */
	public QueryEngine(AerospikeClient client) {
		super();
		this.client = client;
		this.updatePolicy = new WritePolicy(this.client.writePolicyDefault);
		this.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
		this.insertPolicy = new WritePolicy(this.client.writePolicyDefault);
		this.insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		refreshCluster();
		registerUDF();
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
			stmt.setAggregateFunction(this.getClass().getClassLoader(), "com/aerospike/helper/udf/as_utility.lua", "as_utility", "select_records", Value.get(originArgs));
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
	 * Insert
	 * 
	 * ***************************************************** 
	 */
	
	public void insert(String namespace, String set, Key key, List<Bin> bins){
		
		this.client.put(this.insertPolicy, key, bins.toArray(new Bin[0]));	

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
			stmt.setAggregateFunction(this.getClass().getClassLoader(), "com/aerospike/helper/udf/as_utility.lua", "as_utility", "query_meta", Value.get(originArgs));
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
			WritePolicy up = new WritePolicy(updatePolicy);
			up.generation = keyRecord.record.generation;
			try {
				client.put(up, keyRecord.key, bins.toArray(new Bin[0]));
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

	/*
	 * *****************************************************
	 * 
	 * Delete
	 * 
	 * ***************************************************** 
	 */
	public Map<String, Long> delete(String namespace, String set, List<Bin> bins, Filter filter, Qualifier... qualifiers){
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (filter != null)
			stmt.setFilters(filter);
		return update(stmt, bins, qualifiers);	

	}
	public Map<String, Long> delete(Statement stmt, List<Bin> bins, Qualifier... qualifiers){
		KeyRecordIterator results = null;

		if (qualifiers != null && qualifiers.length > 0) {
			Map<String, Object> originArgs = new HashMap<String, Object>();
			originArgs.put("includeAllFields", 1);
			String filterFuncStr = buildFilterFunction(qualifiers);
			originArgs.put("filterFuncStr", filterFuncStr);
			stmt.setAggregateFunction(this.getClass().getClassLoader(), "com/aerospike/helper/udf/as_utility.lua", "as_utility", "query_meta", Value.get(originArgs));
			ResultSet resultSet = this.client.queryAggregate(null, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), resultSet);
		} else {
			RecordSet recordSet = this.client.query(null, stmt);
			results = new KeyRecordIterator(stmt.getNamespace(), recordSet);
		} 
		return update(results, bins);
	}

	private Map<String, Long> delete(KeyRecordIterator results, List<Bin> bins){
		long readCount = 0;
		long updateCount = 0;
		while (results.hasNext()){
			KeyRecord keyRecord = results.next();
			readCount++;
			try {
				client.delete(null, keyRecord.key);
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
		if (getModule("as_utility.lua") == null){ // register the as_utility udf module

			this.client.register(null, this.getClass().getClassLoader(), 
					"com/aerospike/helper/udf/as_utility.lua", 
					"as_utility.lua", Language.LUA);

		}
	}

	public InfoPolicy getInfoPolicy(){
		if (this.infoPolicy == null){
			this.infoPolicy = new InfoPolicy();
		}
		return this.infoPolicy;
	}

	public void refreshCluster(){
		Node[] nodes = client.getNodes();
		for (Node node : nodes){
			try {
				refreshIndexes(node);
				refreshModules(node);
				break;
			} catch (AerospikeException e) {
				log.error("Error geting Index informaton", e);
			}	
		}
	}
	public synchronized void refreshIndexes(Node node){
		/*
		 * cache index by Bin name
		 */
		if (this.indexCache == null)
			this.indexCache = new TreeMap<String, Index>();
		String indexString = Info.request(getInfoPolicy(), node, "sindex");
		if (!indexString.isEmpty()){
			String[] indexList = indexString.split(";");
			for (String oneIndexString : indexList){
				Index index = new Index(oneIndexString);	
				this.indexCache.put(index.getBin(), index);
			}
		}
	}
	
	public synchronized Index getIndex(String binName){
		return this.indexCache.get(binName);
	}

	public synchronized void refreshModules(Node node){
		if (this.moduleCache == null)
			this.moduleCache = new TreeMap<String, Module>();
		String packagesString = Info.request(infoPolicy, node, "udf-list");
		if (!packagesString.isEmpty()){
			String[] packagesList = packagesString.split(";");
			for (String pkgString : packagesList){
				Module module = new Module(pkgString);
				String udfString = Info.request(infoPolicy, node, "udf-get:filename=" + module.getName());
				module.setDetailInfo(udfString);//gen=qgmyp0d8hQNvJdnR42X3BXgUGPE=;type=LUA;recordContent=bG9jYWwgZnVuY3Rpb24gcHV0QmluKHIsbmFtZSx2YWx1ZSkKICAgIGlmIG5vdCBhZXJvc3Bpa2U6ZXhpc3RzKHIpIHRoZW4gYWVyb3NwaWtlOmNyZWF0ZShyKSBlbmQKICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgYWVyb3NwaWtlOnVwZGF0ZShyKQplbmQKCi0tIFNldCBhIHBhcnRpY3VsYXIgYmluCmZ1bmN0aW9uIHdyaXRlQmluKHIsbmFtZSx2YWx1ZSkKICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCmVuZAoKLS0gR2V0IGEgcGFydGljdWxhciBiaW4KZnVuY3Rpb24gcmVhZEJpbihyLG5hbWUpCiAgICByZXR1cm4gcltuYW1lXQplbmQKCi0tIFJldHVybiBnZW5lcmF0aW9uIGNvdW50IG9mIHJlY29yZApmdW5jdGlvbiBnZXRHZW5lcmF0aW9uKHIpCiAgICByZXR1cm4gcmVjb3JkLmdlbihyKQplbmQKCi0tIFVwZGF0ZSByZWNvcmQgb25seSBpZiBnZW4gaGFzbid0IGNoYW5nZWQKZnVuY3Rpb24gd3JpdGVJZkdlbmVyYXRpb25Ob3RDaGFuZ2VkKHIsbmFtZSx2YWx1ZSxnZW4pCiAgICBpZiByZWNvcmQuZ2VuKHIpID09IGdlbiB0aGVuCiAgICAgICAgcltuYW1lXSA9IHZhbHVlCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgZW5kCmVuZAoKLS0gU2V0IGEgcGFydGljdWxhciBiaW4gb25seSBpZiByZWNvcmQgZG9lcyBub3QgYWxyZWFkeSBleGlzdC4KZnVuY3Rpb24gd3JpdGVVbmlxdWUocixuYW1lLHZhbHVlKQogICAgaWYgbm90IGFlcm9zcGlrZTpleGlzdHMocikgdGhlbiAKICAgICAgICBhZXJvc3Bpa2U6Y3JlYXRlKHIpIAogICAgICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFZhbGlkYXRlIHZhbHVlIGJlZm9yZSB3cml0aW5nLgpmdW5jdGlvbiB3cml0ZVdpdGhWYWxpZGF0aW9uKHIsbmFtZSx2YWx1ZSkKICAgIGlmICh2YWx1ZSA+PSAxIGFuZCB2YWx1ZSA8PSAxMCkgdGhlbgogICAgICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCiAgICBlbHNlCiAgICAgICAgZXJyb3IoIjEwMDA6SW52YWxpZCB2YWx1ZSIpIAogICAgZW5kCmVuZAoKLS0gUmVjb3JkIGNvbnRhaW5zIHR3byBpbnRlZ2VyIGJpbnMsIG5hbWUxIGFuZCBuYW1lMi4KLS0gRm9yIG5hbWUxIGV2ZW4gaW50ZWdlcnMsIGFkZCB2YWx1ZSB0byBleGlzdGluZyBuYW1lMSBiaW4uCi0tIEZvciBuYW1lMSBpbnRlZ2VycyB3aXRoIGEgbXVsdGlwbGUgb2YgNSwgZGVsZXRlIG5hbWUyIGJpbi4KLS0gRm9yIG5hbWUxIGludGVnZXJzIHdpdGggYSBtdWx0aXBsZSBvZiA5LCBkZWxldGUgcmVjb3JkLiAKZnVuY3Rpb24gcHJvY2Vzc1JlY29yZChyLG5hbWUxLG5hbWUyLGFkZFZhbHVlKQogICAgbG9jYWwgdiA9IHJbbmFtZTFdCgogICAgaWYgKHYgJSA5ID09IDApIHRoZW4KICAgICAgICBhZXJvc3Bpa2U6cmVtb3ZlKHIpCiAgICAgICAgcmV0dXJuCiAgICBlbmQKCiAgICBpZiAodiAlIDUgPT0gMCkgdGhlbgogICAgICAgIHJbbmFtZTJdID0gbmlsCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgICAgIHJldHVybgogICAgZW5kCgogICAgaWYgKHYgJSAyID09IDApIHRoZW4KICAgICAgICByW25hbWUxXSA9IHYgKyBhZGRWYWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFNldCBleHBpcmF0aW9uIG9mIHJlY29yZAotLSBmdW5jdGlvbiBleHBpcmUocix0dGwpCi0tICAgIGlmIHJlY29yZC50dGwocikgPT0gZ2VuIHRoZW4KLS0gICAgICAgIHJbbmFtZV0gPSB2YWx1ZQotLSAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQotLSAgICBlbmQKLS0gZW5kCg==;
				this.moduleCache.put(module.getName(), module);
			}
		}
	}
	
	public synchronized Module getModule(String moduleName){
		return this.moduleCache.get(moduleName);
	}

}
