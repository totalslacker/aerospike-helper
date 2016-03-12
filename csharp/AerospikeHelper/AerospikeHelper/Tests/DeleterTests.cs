using System;
using System.Collections.Generic;
using NUnit.Framework;
using Aerospike.Client;
using Aerospike.Helper.Query;

namespace Aerospike.Helper.Query
{
	[TestFixture]
	public class DeleterTests : HelperTests
	{
		public DeleterTests () :base ()
		{
		}


		[TestCase]
		public void DeleteByKey(){
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
				String keyString = "selector-test:"+x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(Value.Get(keyString));
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;
				IDictionary<String, long> counts = queryEngine.Delete(stmt, kq);
				Assert.AreEqual((long)1L, (long)counts["write"]);
				Record record = this.client.Get(null, key);
				Assert.IsNull(record);
			}
		}
		[TestCase]
		public void DeleteByDigest(){
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
				String keyString = "selector-test:"+x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(key.digest);
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;
				IDictionary<String, long> counts = queryEngine.Delete(stmt, kq);
				Assert.AreEqual((long)1L, (long)counts["write"]);
				Record record = this.client.Get(null, key);
				Assert.IsNull(record);
			}
		}
		[TestCase]
		public void DeleteStartsWith() {
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.Get("e"));
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<String, long> counts = queryEngine.Delete(stmt, qual1);
			//System.out.println(counts);
			//Assert.AreEqual((long)400L, (long)counts["read"]);
			//Assert.AreEqual((long)400L, (long)counts["write"]);

		}
		[TestCase]
		public void DeleteEndsWith() {
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.Get("na"));
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<String, long> counts = queryEngine.Delete(stmt, qual1, qual2);
			//System.out.println(counts);
			Assert.AreEqual((long)200L, (long)counts["read"]);
			Assert.AreEqual((long)200L, (long)counts["write"]);
		}
		[TestCase]
		public void DeleteWithFilter() {
			Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "first-name-1");
			Bin firstNameBin = new Bin("first_name", "first-name-1");
			Bin lastNameBin = new Bin("last_name", "last-name-1");
			int age = 25;
			Bin ageBin = new Bin("age", age);
			this.client.Put(null, key, firstNameBin, lastNameBin, ageBin);

			Qualifier qual1 = new Qualifier("last_name", Qualifier.FilterOperation.EQ, Value.Get("last-name-1"));
			//DELETE FROM test.people WHERE last_name='last-name-1'
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<String, long> counts = queryEngine.Delete(stmt, qual1);
			Assert.AreEqual((long)1L, (long)counts["read"]);
			Assert.AreEqual((long)1L, (long)counts["write"]);
			Record record = this.client.Get(null, key);
			Assert.IsNull(record);
		}

	}
}

