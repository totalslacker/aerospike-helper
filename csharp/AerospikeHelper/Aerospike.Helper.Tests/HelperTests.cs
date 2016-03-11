using System;
using Aerospike.Client;
using Aerospike.Helper.Query;
using NUnit.Framework;

namespace Aerospike.Helper.Tests
{
	[SetUpFixture]
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
			clientPolicy = new ClientPolicy();
			clientPolicy.timeout = TestQueryEngine.TIME_OUT;
			client = new AerospikeClient(clientPolicy, TestQueryEngine.HOST, TestQueryEngine.PORT);
			client.writePolicyDefault.expiration = 1800;
			client.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;

		}

		[SetUp]
		public void SetUp() {
			try
			{
				queryEngine = new QueryEngine(client);
				int i = 0;
				Key key = new Key(TestQueryEngine.NAMESPACE, TestQueryEngine.SET_NAME, "selector-test:"+ 10);
				if (this.client.Exists(null, key))
					return;
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
			}	catch (Exception ex)
				{
					caughtException = ex;               
				}
		}

		[TearDown]
		public void TearDown() {
			queryEngine.Close();
			if (caughtException != null)
			{
				Console.WriteLine(string.Format("TestFixtureSetUp failed in {0} - {1}", this.GetType(), caughtException.Message));
			} 
		}

	}
}

