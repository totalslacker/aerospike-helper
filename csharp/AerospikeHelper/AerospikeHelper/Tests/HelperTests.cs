using System;
using Aerospike.Client;
using Aerospike.Helper.Query;
using NUnit.Framework;

namespace Aerospike.Helper.Query
{
	[TestFixture]
	public class HelperTests
	{
		protected AerospikeClient client;
		protected ClientPolicy clientPolicy;
		protected QueryEngine queryEngine;
		protected int[] ages = new int[]{25,26,27,28,29};
		protected String[] colours = new String[]{"blue","red","yellow","green","orange"};
		protected String[] animals = new String[]{"cat","dog","mouse","snake","lion"};

		private Exception caughtException = null;

		public HelperTests ()
		{
			
		}

		[SetUp]
		public virtual void SetUp() {
			try
			{
				Console.WriteLine("Creating AerospikeClient");
				clientPolicy = new ClientPolicy();
				clientPolicy.timeout = TestQueryEngine.TIME_OUT;
				client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
				client.writePolicyDefault.expiration = 1800;
				client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;
				Console.WriteLine("Creating QueryEngine");
				queryEngine = new QueryEngine(client);
				int i = 0;
				Console.WriteLine("Creating Test Data");
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ 10);
//				if (!this.client.Exists(null, key)){
					for (int x = 1; x <= TestQueryEngine.RECORD_COUNT; x++){
						key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ x);
						Bin name = new Bin("name", "name:" + x);
						Bin age = new Bin("age", ages[i]);
						Bin colour = new Bin("color", colours[i]);
						Bin animal = new Bin("animal", animals[i]);
						this.client.Put(null, key, name, age, colour, animal);
						i++;
						if ( i == 5)
							i = 0;
					}
//				}
				Console.WriteLine("Created Test Data");
			}	catch (Exception ex)
				{
					caughtException = ex; 
					Console.WriteLine(string.Format("TestFixtureSetUp failed in {0} - {1} {2}", this.GetType(), caughtException.GetType(), caughtException.Message));
					Console.WriteLine (caughtException.StackTrace);
				}
		}

		[TearDown]
		public virtual void TearDown() {
			Console.WriteLine("ClosingQueryEngine");
			queryEngine.Close();
			if (caughtException != null)
			{
				Console.WriteLine(string.Format("TestFixtureSetUp failed in {0} - {1} {2}", this.GetType(), caughtException.GetType(), caughtException.Message));
			} 
		}

	}
}

