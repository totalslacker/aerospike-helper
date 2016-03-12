# QueryEngine

The Query engine class uses an instance of an `AerospikeClient` to implement operations familiar to most developers. A QueryEngine instance contain no state and is thread safe. 

Each operation of 
 - Select
 - Insert
 - Update
 - Delete

Zero or More filters (predicates) can be specified for each operation. Filtering is done in server in the Aerospike cluster.

##Creating a query engine
Creating a QueryEngine is quite simple, the constructor is passed an instance of AerospikeClient.

```C#
	AerospikeClient client = new AerospikeClient(clientPolicy, "127.0.0.1", 3000);
	
	. . .
	
	QueryEngine queryEngine = new QueryEngine(client);

```


## Select example

Records are selected using a list of the Criteria objects.  In this example you can see the equivalent of this sql statement:
```sql
	SELECT * FROM test.selector WHERE color = 'blue' AND name LIKE 'na'
```
C#
```C#
```
## Insert example
This example is an insert equivalent to this sql statement:
```sql
	INSERT INTO test.selector (PK,name,age,color,animal) 
	VALUES (keyString,value1,value2,value3,value4)
```
C#
```C#

```
## Update example
Here is a update example that uses a filter, similar to this SQL:
```sql
	UPDATE test.selector
	SET ending ='ends with e'
	WHERE color LIKE 'e';
```
C#
```C#
```
## Delete example
The Delete example that uses a filter, similar to this SQL:
```sql
	DELETE FROM test.people WHERE last_name='last-name-1'
```
C#
```C#
```


