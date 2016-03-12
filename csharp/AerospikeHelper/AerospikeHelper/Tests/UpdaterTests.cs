using System;
using System.Collections.Generic;
using NUnit.Framework;
using Aerospike.Client;
using Aerospike.Helper.Query;


namespace Aerospike.Helper.Query
{
	[TestFixture]
	public class UpdaterTests : HelperTests
	{
		public UpdaterTests () : base()
		{
		}

		[TestCase]
		public void UpdateByKey(){
			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
				String keyString = "selector-test:"+x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(Value.Get(keyString));
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;

				List<Bin> bins = new List<Bin>() {
						new Bin("ending", "ends with e")
					};

				IDictionary<String, long> counts = queryEngine.Update(stmt, bins, kq );
				Assert.AreEqual((long)1L, (long)counts["write"]);
				Record record = this.client.Get(null, key);
				Assert.NotNull(record);
				String ending = record.GetString("ending");
				Assert.True(ending.EndsWith("ends with e"));
			}
		}
		[TestCase]
		public void UpdateByDigest(){

			for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
				String keyString = "selector-test:"+x;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, keyString);
				KeyQualifier kq = new KeyQualifier(key.digest);
				Statement stmt = new Statement();
				stmt.Namespace = TestQueryEngine.NAMESPACE;
				stmt.SetName = TestQueryEngine.SET_NAME;

				List<Bin> bins = new List<Bin>() {
						new Bin("ending", "ends with e")
					};

				IDictionary<String, long> counts = queryEngine.Update(stmt, bins, kq );
				Assert.AreEqual((long)1L, (long)counts["write"]);
				Record record = this.client.Get(null, key);
				Assert.NotNull(record);
				String ending = record.GetString("ending");
				Assert.True(ending.EndsWith("ends with e"));
			}
		}
		[TestCase]
		public void UpdateStartsWith() {
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.Get("e"));
			List<Bin> bins = new List<Bin>() {
					new Bin("ending", "ends with e")
				};
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<String, long> counts = queryEngine.Update(stmt, bins, qual1);
			//System.out.println(counts);
			Assert.AreEqual((long)400L, (long)counts["read"]);
			Assert.AreEqual((long)400L, (long)counts["write"]);
		}

		[TestCase]
		public void UpdateEndsWith() {
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.Get("na"));
			List<Bin> bins = new List<Bin>() {
					new Bin("starting", "ends with e")
				};
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			IDictionary<String, long> counts = queryEngine.Update(stmt, bins, qual1, qual2);
			//System.out.println(counts);
			Assert.AreEqual((long)200L, (long)counts["read"]);
			Assert.AreEqual((long)200L, (long)counts["write"]);
		}

	}
}

