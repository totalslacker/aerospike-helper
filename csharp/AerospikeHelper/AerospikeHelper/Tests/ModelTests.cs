using System;
using NUnit.Framework;
using Aerospike.Client;
using System.Collections.Generic;

namespace Aerospike.Helper.Model
{
	[TestFixture]
	public class ModelTests

	{
		AerospikeClient client = new AerospikeClient ("127.0.0.1", 3000);
		public ModelTests ()
		{
		}

		[TestCase]
		public void RefreshCluster ()
		{
			
			Console.WriteLine ("Namespace refresh");



			try {
				
				SortedDictionary<String, Namespace> namespaceCache = new SortedDictionary<String, Namespace> ();
				foreach (Node node in client.Nodes) {

					String namespaceString = Aerospike.Client.Info.Request (node, "namespaces");
					if (namespaceString.Trim ().Length > 0) {
						String[] namespaceList = namespaceString.Split (';');
						foreach (string nss in namespaceList) {
							Namespace ns = null;
							if (!namespaceCache.TryGetValue (nss, out ns)) {
								ns = new Namespace (nss);
								namespaceCache [nss] = ns;
							}
							RefreshNamespaceData (node, ns);
						}
					}

				}

			} catch (AerospikeException e) {
				Console.WriteLine (String.Format ("Error geting Namespaces, Result code {0}, Message: {1} ", e.Message, e.Result));
				Console.WriteLine (e.StackTrace);
			}	finally {
					Console.WriteLine ("namespace refresh completed");
			}
		}

		public void RefreshNamespaceData (Node node, Namespace ns)
		{
			/*
		 * refresh namespace data
		 */
			try {
				String nameSpaceString = Aerospike.Client.Info.Request (node, "namespace/" + ns);
				ns.MergeNamespaceInfo (nameSpaceString);
				String setsString = Aerospike.Client.Info.Request (null, node, "sets/" + ns);
				if (setsString.Length > 0) {
					String[] sets = setsString.Split (';');
					foreach (String setData in sets) {
						ns.MergeSet (setData);
					}
				}
			} catch (AerospikeException e) {
				Console.WriteLine ("Error geting Namespace details", e);
			}	
		}
		[TestCase]
		public void RefreshIndexes ()
		{
			SortedDictionary<String, Index> indexCache = new SortedDictionary<String, Index> ();

			foreach (Node node in client.Nodes) {
				if (node.Active) {
					try {
						String indexString = Info.Request (node, "sindex");
						Console.WriteLine ("Info: " + indexString);
						if (indexString.Length > 0) {
							String[] indexList = indexString.Split (';');
							foreach (String oneIndexString in indexList) {
								Console.WriteLine ("\tInfo: " + oneIndexString);
								Index index = new Index (oneIndexString);	
								Console.WriteLine ("\tIndex: " + index,ToString());
								String indexBin = index.Bin;
								indexCache [indexBin] = index;
							}
						}
						break;
					} catch (AerospikeException e) {
						Console.WriteLine ("Error geting Index informaton", e);
					}	
				}
			}
		}

		[TestCase]
		public void RefreshModules ()
		{
			SortedDictionary<String, Module> moduleCache = new SortedDictionary<String, Module> ();
			foreach (Node node in client.Nodes) {
				if (node.Active) {
					String packagesString = Info.Request (node, "udf-list");
					Console.WriteLine ("Info: " + packagesString);
					if (packagesString.Length > 0) {
						String[] packagesList = packagesString.Split (';');
						foreach (String pkgString in packagesList) {
							Console.WriteLine ("\tInfo: " + pkgString);
							Module module = new Module (pkgString);
							String udfString = Info.Request (node, "udf-get:filename=" + module.Name);
							module.DetailInfo (udfString);//gen=qgmyp0d8hQNvJdnR42X3BXgUGPE=;type=LUA;recordContent=bG9jYWwgZnVuY3Rpb24gcHV0QmluKHIsbmFtZSx2YWx1ZSkKICAgIGlmIG5vdCBhZXJvc3Bpa2U6ZXhpc3RzKHIpIHRoZW4gYWVyb3NwaWtlOmNyZWF0ZShyKSBlbmQKICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgYWVyb3NwaWtlOnVwZGF0ZShyKQplbmQKCi0tIFNldCBhIHBhcnRpY3VsYXIgYmluCmZ1bmN0aW9uIHdyaXRlQmluKHIsbmFtZSx2YWx1ZSkKICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCmVuZAoKLS0gR2V0IGEgcGFydGljdWxhciBiaW4KZnVuY3Rpb24gcmVhZEJpbihyLG5hbWUpCiAgICByZXR1cm4gcltuYW1lXQplbmQKCi0tIFJldHVybiBnZW5lcmF0aW9uIGNvdW50IG9mIHJlY29yZApmdW5jdGlvbiBnZXRHZW5lcmF0aW9uKHIpCiAgICByZXR1cm4gcmVjb3JkLmdlbihyKQplbmQKCi0tIFVwZGF0ZSByZWNvcmQgb25seSBpZiBnZW4gaGFzbid0IGNoYW5nZWQKZnVuY3Rpb24gd3JpdGVJZkdlbmVyYXRpb25Ob3RDaGFuZ2VkKHIsbmFtZSx2YWx1ZSxnZW4pCiAgICBpZiByZWNvcmQuZ2VuKHIpID09IGdlbiB0aGVuCiAgICAgICAgcltuYW1lXSA9IHZhbHVlCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgZW5kCmVuZAoKLS0gU2V0IGEgcGFydGljdWxhciBiaW4gb25seSBpZiByZWNvcmQgZG9lcyBub3QgYWxyZWFkeSBleGlzdC4KZnVuY3Rpb24gd3JpdGVVbmlxdWUocixuYW1lLHZhbHVlKQogICAgaWYgbm90IGFlcm9zcGlrZTpleGlzdHMocikgdGhlbiAKICAgICAgICBhZXJvc3Bpa2U6Y3JlYXRlKHIpIAogICAgICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFZhbGlkYXRlIHZhbHVlIGJlZm9yZSB3cml0aW5nLgpmdW5jdGlvbiB3cml0ZVdpdGhWYWxpZGF0aW9uKHIsbmFtZSx2YWx1ZSkKICAgIGlmICh2YWx1ZSA+PSAxIGFuZCB2YWx1ZSA8PSAxMCkgdGhlbgogICAgICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCiAgICBlbHNlCiAgICAgICAgZXJyb3IoIjEwMDA6SW52YWxpZCB2YWx1ZSIpIAogICAgZW5kCmVuZAoKLS0gUmVjb3JkIGNvbnRhaW5zIHR3byBpbnRlZ2VyIGJpbnMsIG5hbWUxIGFuZCBuYW1lMi4KLS0gRm9yIG5hbWUxIGV2ZW4gaW50ZWdlcnMsIGFkZCB2YWx1ZSB0byBleGlzdGluZyBuYW1lMSBiaW4uCi0tIEZvciBuYW1lMSBpbnRlZ2VycyB3aXRoIGEgbXVsdGlwbGUgb2YgNSwgZGVsZXRlIG5hbWUyIGJpbi4KLS0gRm9yIG5hbWUxIGludGVnZXJzIHdpdGggYSBtdWx0aXBsZSBvZiA5LCBkZWxldGUgcmVjb3JkLiAKZnVuY3Rpb24gcHJvY2Vzc1JlY29yZChyLG5hbWUxLG5hbWUyLGFkZFZhbHVlKQogICAgbG9jYWwgdiA9IHJbbmFtZTFdCgogICAgaWYgKHYgJSA5ID09IDApIHRoZW4KICAgICAgICBhZXJvc3Bpa2U6cmVtb3ZlKHIpCiAgICAgICAgcmV0dXJuCiAgICBlbmQKCiAgICBpZiAodiAlIDUgPT0gMCkgdGhlbgogICAgICAgIHJbbmFtZTJdID0gbmlsCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgICAgIHJldHVybgogICAgZW5kCgogICAgaWYgKHYgJSAyID09IDApIHRoZW4KICAgICAgICByW25hbWUxXSA9IHYgKyBhZGRWYWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFNldCBleHBpcmF0aW9uIG9mIHJlY29yZAotLSBmdW5jdGlvbiBleHBpcmUocix0dGwpCi0tICAgIGlmIHJlY29yZC50dGwocikgPT0gZ2VuIHRoZW4KLS0gICAgICAgIHJbbmFtZV0gPSB2YWx1ZQotLSAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQotLSAgICBlbmQKLS0gZW5kCg==;
							moduleCache [module.Name] = module;
						}
					}
					break;
				}
			}
		}

	}
}

