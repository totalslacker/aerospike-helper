# Aerospike helper
Unofficial helper functions and classes for Aerospike DB

It includes
- Query Engine 
- UDF utility functions

## Query Engine
The `QueryEnginer` is a multi-filter query engine in Java using Aerospike Aggregations. A query will automatically choose an index if one is available to qualify the results, and then use Stream UDFs to further qualify the results.

The `QueryEngine` uses a `Statement` and zero or mode `Qualifier` objects and produces a closable `KeyRecordIterator` to iterate over the results of the query.

### Select example
```java
Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.get("blue"));
		Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.get("na"));
		KeyRecordIterator it = queryEngine.select(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, null, qual1, qual2);
		try{
			while (it.hasNext()){
				KeyRecord rec = it.next();
				Assert.assertEquals("blue", rec.record.getString("color"));
				Assert.assertTrue(rec.record.getString("name").startsWith("na"));
			}
		} finally {
			it.close();
		}
```
### Insert example
```java
			Bin name = new Bin("name", "name:" + x);
			Bin age = new Bin("age", ages[i]);
			Bin colour = new Bin("color", colours[i]);
			Bin animal = new Bin("animal", animals[i]);
			List<Bin> bins = Arrays.asList(name, age, colour, animal);
			
			Key key = new Key(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, keyString);
			KeyQualifier kq = new KeyQualifier(key.digest);
			Statement stmt = new Statement();
			stmt.setNamespace(QueryEngineTests.NAMESPACE);
			stmt.setSetName(QueryEngineTests.SET_NAME);
			
			queryEngine.insert(stmt, kq, bins);

```
### Update example
```java
		Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.get("e"));
		ArrayList<Bin> bins = new ArrayList<Bin>() {{
		    add(new Bin("ending", "ends with e"));
		}};
		Statement stmt = new Statement();
		stmt.setNamespace(QueryEngineTests.NAMESPACE);
		stmt.setSetName(QueryEngineTests.SET_NAME);
		Map<String, Long> counts = queryEngine.update(stmt, bins, qual1);
		//System.out.println(counts);
		Assert.assertEquals((Long)40L, (Long)counts.get("read"));
		Assert.assertEquals((Long)40L, (Long)counts.get("write"));
```
### Delete example
```java
Key key = new Key(QueryEngineTests.NAMESPACE, QueryEngineTests.SET_NAME, "first-name-1");
		Bin firstNameBin = new Bin("first_name", "first-name-1");
		Bin lastNameBin = new Bin("last_name", "last-name-1");
		int age = 25;
		Bin ageBin = new Bin("age", age);
		this.client.put(null, key, firstNameBin, lastNameBin, ageBin);

		Qualifier qual1 = new Qualifier("last_name", Qualifier.FilterOperation.EQ, Value.get("last-name-1"));
		//DELETE FROM test.people WHERE last_name='last-name-1'
		Statement stmt = new Statement();
		stmt.setNamespace(QueryEngineTests.NAMESPACE);
		stmt.setSetName(QueryEngineTests.SET_NAME);
		Map<String, Long> counts = queryEngine.delete(stmt, qual1);
```


## UDF utility functions
The `as_utility` module contains a number of functions:
- udf debuging
- multi predicate queries
 
### Debugging functions
These are primative functions that print information to the aerospike log

#### dumpTable (tbl, indent)
This functions prints a Lua table to the Aerospike log. The parameters are:
- *tbl* the table to be printed
- *indent* the indent level

#### dumpRecord (rec)
This function prints an Aerospike record to the Aerospike log. The prameter is:
- *rec* the record to be printed. It must alread exist.

#### dumpLocal()
This function prints all the local variables, for the current scope, to the Aerospike log

#### filter_record(rec, filterFuncStr, filterFunc)
This is function is used by the query stream to implement multiple filters. Do not call this directly.

#### parseFieldStatements(fieldValueStatements)
This function parses the fields to be used in the query stream. Do not call this directly.
 
#### select_records(stream, origArgs)
This function selects records from the stream based on the criteria specified in `origArgs`. This function is call by the the `QueryEngine` as part of the query stream processing ro redutn records as a RecordSet to the client application.

#### query_meta(stream, origArgs)
This function is similar to `select_records` but it only produces metadata. It is used by the `QueryEngine` for `update` and ` delete` operations where only the digest and generation are required.



