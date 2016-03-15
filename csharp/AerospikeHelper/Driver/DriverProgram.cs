using System;
using System.Collections.Generic;
using NUnit.Framework;

using Aerospike.Helper.Query;
using Aerospike.Client;

namespace Driver
{
	class DriverProgram
	{
		AerospikeClient client;
		QueryEngine queryEngine;

		public DriverProgram(AerospikeClient client, QueryEngine queryEngine){
			this.client = client;
			this.queryEngine = queryEngine;
		}

		public static void Main (string[] args)
		{
			Console.WriteLine ("Helper test app");
			Console.WriteLine (Environment.CurrentDirectory);
			AerospikeClient client = new AerospikeClient ("127.0.0.1", 3000);
			QueryEngine qe = new QueryEngine (client);

			DriverProgram dp = new DriverProgram (client, qe);
			dp.CreateTestData ();
			Console.WriteLine ("SelectAll();");
			dp.SelectAll();

			Console.WriteLine ("SelectOnIndex();");
			//dp.SelectOnIndex ();

			Console.WriteLine ("SelectOnIndexWithQualifiers();");
			dp.SelectOnIndexWithQualifiers ();

			Console.WriteLine ("SelectWithGeneration();");
			dp.SelectWithGeneration ();

			Console.WriteLine ("SelectWithQualifiersOnly();");
			dp.SelectWithQualifiersOnly ();

			Console.WriteLine ("SelectStartsWith();");
			dp.SelectStartsWith ();

			Console.WriteLine ("SelectEndsWith();");
			dp.SelectEndsWith();
		}
		public void CreateTestData(){
			int[] ages = new int[]{25,26,27,28,29};
			String[] colours = new String[]{"blue","red","yellow","green","orange"};
			String[] animals = new String[]{"cat","dog","mouse","snake","lion"};
			int i = 0;
			Console.WriteLine ("Creating Test Data");
			Key key = new Key (TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:" + 10);
			if (!client.Exists (null, key)) {
				for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++) {
					key = new Key (TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:" + x);
					Bin name = new Bin ("name", "name:" + x);
					Bin age = new Bin ("age", ages [i]);
					Bin colour = new Bin ("color", colours [i]);
					Bin animal = new Bin ("animal", animals [i]);
					client.Put (null, key, name, age, colour, animal);
					i++;
					if (i == 5)
						i = 0;
				}
			}
			Console.WriteLine ("Created Test Data");

		}

		public void SelectAll() {
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null);
			try {
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
				}
			} finally {
				it.Close();
			}
		}

		 
		public void SelectOnIndex() {
			this.client.CreateIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index", "age", IndexType.NUMERIC);

			Filter filter = Filter.Range("age", 28, 29);
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter);
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
	 
		public void SelectStartsWith() {
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.ENDS_WITH, Value.Get("e"));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
			try{
				while (it.MoveNext()){
					KeyRecord rec = (KeyRecord)it.Current;
					Assert.IsTrue(rec.record.GetString("color").EndsWith("e"));

				}
			} finally {
				it.Close();
			}
		}
		 
		public void SelectEndsWith() {
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			Qualifier qual2 = new Qualifier("name", Qualifier.FilterOperation.START_WITH, Value.Get("na"));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
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
		 
		public void SelectOnIndexWithQualifiers() {
			this.client.CreateIndex(null, TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "age_index_selector", "age", IndexType.NUMERIC);

			Filter filter = Filter.Range("age", 25, 29);
			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("blue"));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, filter, qual1);
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
		 
		public void SelectWithQualifiersOnly() {
			queryEngine.refreshCluster();

			Qualifier qual1 = new Qualifier("color", Qualifier.FilterOperation.EQ, Value.Get("green"));
			Qualifier qual2 = new Qualifier("age", Qualifier.FilterOperation.BETWEEN, Value.Get(28), Value.Get(29));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1, qual2);
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

		public void SelectWithGeneration() {
			queryEngine.refreshCluster();
			Qualifier qual1 = new GenerationQualifier(Qualifier.FilterOperation.GTEQ, Value.Get(1));
			KeyRecordEnumerator it = queryEngine.Select(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, null, qual1);
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

