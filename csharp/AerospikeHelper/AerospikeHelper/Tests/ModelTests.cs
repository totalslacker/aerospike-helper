using System;
using NUnit.Framework;
using Aerospike.Client;
using Aerospike.Helper.Query;
using Aerospike.Helper.Tests;

namespace Aerospike.Helper.Model
{
	[TestFixture]
	public class ModelTests

	{
		AerospikeClient client;
		QueryEngine qe;

		public ModelTests ()
		{
			client = new AerospikeClient (TestQueryEngine.HOST, TestQueryEngine.PORT);
			qe = new QueryEngine(client);
		}

		[TestCase]
		public void RefreshCluster ()
		{
			
			qe.refreshCluster ();
			Console.WriteLine ("Namespaces");
			foreach (Namespace ns in qe.Namespaces) {
				Console.WriteLine ("\t"+ns.Name);
				foreach (Set set in ns.Sets) {
					Console.WriteLine ("\t\t"+set.Name);
				}
			}
			Console.WriteLine ("Indexes");
			foreach (Index index in qe.Indexes) {
				Console.WriteLine ("\t"+index.Name);
			}
			Console.WriteLine ("Modules");
			foreach (Aerospike.Helper.Model.Module module in qe.Modules) {
				Console.WriteLine ("\t"+module.Name);
			}

		}

		[TestCase]
		public void RefreshNamespaces ()
		{
			qe.RefreshNamespaces();
		}

		[TestCase]
		public void RefreshIndexes ()
		{
			qe.RefreshIndexes ();

		}

		[TestCase]
		public void RefreshModules ()
		{
			qe.RefreshModules ();
		}

	}
}

