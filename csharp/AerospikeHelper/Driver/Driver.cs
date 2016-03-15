using System;
using Aerospike.Helper.Query;
using Aerospike.Client;

namespace Aerospike.Helper.Driver
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			Console.WriteLine ("Helper test app");
			AerospikeClient client = new AerospikeClient ("127.0.0.1", 3000);
			QueryEngine qe = new QueryEngine (client);
			CreateTestData (client);
		}
		public static void CreateTestData(AerospikeClient client){
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
	}
}
