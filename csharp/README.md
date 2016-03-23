# Aerospike helper
Unofficial helper functions and classes for Aerospike

Aerospike Helper includes:
- LargeList
- Query Engine 
- UDF utility functions

## LargeList

[LargeList Documentation](doc/LargeList.md)

## Query Engine - Work in progress
The `QueryEnginer` is a multi-filter query engine in Java using Aerospike Aggregations. A query will automatically choose an index if one is available to qualify the results, and then use Stream UDFs to further qualify the results.

The `QueryEngine` uses a `Statement` and zero or mode `Qualifier` objects and produces a closable `KeyRecordIterator` to iterate over the results of the query.

C# namespace `Aerospike.Helper.Query`

[QueryEngine Documentation](doc/query.md)


## UDF utility functions
The `as_utility` Lua module contains a number of functions for:
- udf debuging
- multi predicate queries

Lua directory `lua`

[UDF Documentation](doc/udf.md)
 
