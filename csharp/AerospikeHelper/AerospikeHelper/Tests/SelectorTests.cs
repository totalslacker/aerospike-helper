using System;
using NUnit.Framework;
using Aerospike.Client;
using Aerospike.Helper.Query;

namespace Aerospike.Helper.Query
{
	[TestFixture ()]
	public class SelectorTests : HelperTests
	{
		public SelectorTests () : base()
		{
			
		}
		[TestCase]
		public void SelectOneWitKey() {
			Statement stmt = new Statement();
			stmt.Namespace = TestQueryEngine.NAMESPACE;
			stmt.SetName = TestQueryEngine.SET_NAME;
			KeyQualifier kq = new KeyQualifier(Value.Get("selector-test:3"));
			KeyRecordIterator it = queryEngine.Select(stmt, kq);
			int count = 0;
			while (it.MoveNext()){
				KeyRecord rec = (KeyRecord)it.Current;
				count++;
				//			System.out.println(rec);
			}
			it.Close();
			//		System.out.println(count);
			Assert.AreEqual(1, count);
		}

		[TestCase]
		public void SelectAll() {
			KeyRecordIterator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null);
			try {
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
				}
			} finally {
				it.Close();
			}
		}

		[TestCase]
		public void SelectOnIndex() {
			IndexTask task = this.client.CreateIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
			task.Wait();
			Filter filter = Filter.Range("age", 28, 29);
			KeyRecordIterator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter);
			try{
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
					int age = rec.record.GetInt("age");
					Assert.IsTrue(age >= 28 && age <= 29);
				}
			} finally {
				it.Close();
			}
		}
		[TestCase]
		public void SelectStartsWith() {
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.Get("e"));
			KeyRecordIterator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
			try{
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
					Assert.IsTrue(rec.record.GetString("color").EndsWith("e"));

				}
			} finally {
				it.Close();
			}
		}
		[TestCase]
		public void SelectEndsWith() {
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.Get("na"));
			KeyRecordIterator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
			try{
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
					Assert.AreEqual("blue", rec.record.GetString("color"));
					Assert.IsTrue(rec.record.GetString("name").StartsWith("na"));
				}
			} finally {
				it.Close();
			}
		}
		[TestCase]
		public void SelectOnIndexWithQualifiers() {
			IndexTask task = this.client.CreateIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index_selector", "age", IndexType.NUMERIC);
			task.Wait();
			Filter filter = Filter.Range("age", 25, 29);
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			KeyRecordIterator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter, qual1);
			try{
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
					Assert.AreEqual("blue", rec.record.GetString("color"));
					int age = rec.record.GetInt("age");
					Assert.IsTrue(age >= 25 && age <= 29);
				}
			} finally {
				it.Close();
			}
		}
		[TestCase]
		public void SelectWithQualifiersOnly() {
			IndexTask task = this.client.CreateIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);
			task.Wait();
			queryEngine.refreshCluster();
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("green"));
			Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.Get(28), Value.Get(29));
			KeyRecordIterator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
			try{
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
					Assert.AreEqual("green", rec.record.GetString("color"));
					int age = rec.record.GetInt("age");
					Assert.IsTrue(age >= 28 && age <= 29);
				}
			} finally {
				it.Close();
			}
		}
		[TestCase]
		public void SelectWithGeneration() {
			queryEngine.refreshCluster();
			Qualifier qual1 = new GenerationQualifier(Qualifier.FilterOperation.GTEQ, Value.Get(1));
			KeyRecordIterator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
			try {
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
					Assert.IsTrue(rec.record.generation >= 1);
				}
			} finally {
				it.Close();
			}
		}

	}
}

