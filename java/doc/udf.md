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



