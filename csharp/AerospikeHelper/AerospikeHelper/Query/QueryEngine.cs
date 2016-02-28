using System;
using log4net;
using Aerospike.Client;
using System.Collections.Generic;
using AerospikeHelper.Model;
using System.Runtime.CompilerServices;
using System.Text;


namespace AerospikeHelper.Query
{
	/// <summary>
	/// This class provides a multi-filter query engine that
	/// augments the query capability in Aerospike.
	/// To achieve this the class uses a UserDefined Function written in Lua to 
	/// provide the additional filtering. This UDF module packaged in the JAR and is automatically registered
	/// with the cluster.
	/// </summary>
	public class QueryEngine : IDisposable
	{
		protected const String QUERY_MODULE = "as_utility_1_0";
		//DO NOT use decimal places in the module name

		protected const String AS_UTILITY_PATH = QUERY_MODULE + ".lua";

		protected static readonly ILog log = LogManager.GetLogger (typeof(QueryEngine));

		protected AerospikeClient client;
		protected SortedDictionary<String, Index> indexCache;
		public WritePolicy updatePolicy;
		public WritePolicy insertPolicy;
		public InfoPolicy infoPolicy;

		protected SortedDictionary<String, Module> moduleCache;

		protected SortedDictionary<String, Namespace> namespaceCache;

		public enum Meta
		{
			[StringValue ("__key")] KEY,
			[StringValue ("__TTL")] TTL,
			[StringValue ("__Expiration")] EXPIRATION,
			[StringValue ("__generation")] GENERATION
		}


		public QueryEngine ()
		{
			Value.UseDoubleType = true;
		}

		///
		/// The Query engine is constructed by passing in an existing 
		/// AerospikeClient instance
		/// @param client
		///
		public QueryEngine (AerospikeClient client) : this ()
		{
			
			SetClient (client);
		}

		public void SetClient (AerospikeClient client)
		{
			this.client = client;
			this.updatePolicy = new WritePolicy (this.client.writePolicyDefault);
			this.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
			this.insertPolicy = new WritePolicy (this.client.writePolicyDefault);
			this.insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
			refreshCluster ();
			registerUDF ();
		}

		private String buildSortFunction (IDictionary<String, String> sortMap)
		{
			// TODO Auto-generated method stub
			return null;
		}

		#region Select

		#endregion


	 	#region Insert

		public void Insert(String ns, String set, Key key, List<Bin> bins){

			Insert(ns, set, key, bins, 0)	;

		}

		public void Insert(String ns, String set, Key key, List<Bin> bins, int ttl){

			this.client.Put(this.insertPolicy, key, bins.ToArray());	

		}

		public void Insert(Statement stmt, KeyQualifier keyQualifier, List<Bin> bins){
			Insert(stmt, keyQualifier, bins, 0);
		}

		public void Insert(Statement stmt, KeyQualifier keyQualifier, List<Bin> bins, int ttl){
			Key key = keyQualifier.MakeKey(stmt.Namespace, stmt.SetName);
			//Key key = new Key(stmt.getNamespace(), stmt.getSetName(), keyQualifier.getValue1());
			this.client.Put(this.insertPolicy, key, bins.ToArray());	

		}
		#endregion

		#region Update

		#endregion

		#region Delete
		public IDictionary<String, long> Delete(Statement stmt, params Qualifier[] qualifiers){
			if (qualifiers == null || qualifiers.Length == 0){
				/*
			 * There are no qualifiers, so delete every record in the set
			 * using Scan UDF delete
			 */
				ExecuteTask task = client.Execute(null, stmt, QUERY_MODULE, "delete_record");
				while (!task.IsDone)
					Thread.Sleep (10);
				return null;
			}
			if (qualifiers.Length == 1 && qualifiers[0] is KeyQualifier){
				KeyQualifier keyQualifier = (KeyQualifier) qualifiers[0];
				Key key = keyQualifier.MakeKey(stmt.Namespace, stmt.SetName);
				this.client.Delete(null, key);
				Dictionary<String, long> map = new Dictionary<String, long>();
				map["read"] = 1L;
				map["write"] = 1L;
				return map;
			}
			KeyRecordIterator results = Select(stmt, true, qualifiers);
			return Delete(results);
		}

		public IDictionary<String, long> Delete(KeyRecordIterator results){
			long readCount = 0;
			long updateCount = 0;
			while (results.HasNext){
				KeyRecord keyRecord = results.Next();
				readCount++;
				try {
					if (client.Delete(null, keyRecord.key))
						updateCount++;
				} catch (AerospikeException e){
					log.Error("Unexpected exception deleting "+ keyRecord.key, e);
				}
			}
			Dictionary<String, long> res = new Dictionary<String, long>();
			res["read"] = readCount;
			res["write"] = updateCount;
			return res;
		}

		#endregion

		private String buildFilterFunction (Qualifier[] qualifiers)
		{
			int count = 0;
			StringBuilder sb = new StringBuilder ("if ");
			for (int i = 0; i < qualifiers.Length; i++) {
				if (qualifiers [i] == null) //Skip nulls
					continue;
				if (qualifiers [i] is KeyQualifier) //Skip primary key -- should not happen
					continue;
				if (count > 0)
					sb.Append (" and ");

				sb.Append (qualifiers [i].luaFilterString ());
				count++;
			}
			sb.Append (" then selectedRec = true end");
			return sb.ToString ();
		}

		private void registerUDF ()
		{
			if (GetModule (QUERY_MODULE + ".lua") == null) { // register the as_utility udf module

				RegisterTask task = this.client.Register (null, this.getClass ().getClassLoader (), 
					                    AS_UTILITY_PATH, 
					                    QUERY_MODULE + ".lua", Language.LUA);
				task.IsDone ();
			}
		}

		public InfoPolicy InfoPolicy {
			get {
				
				if (this.infoPolicy == null) {
					this.infoPolicy = new InfoPolicy ();
				}
				return this.infoPolicy;
			}
		}

		public void refreshCluster ()
		{
			RefreshNamespaces ();
			RefreshIndexes ();
			RefreshModules ();
		}

		[MethodImpl (MethodImplOptions.Synchronized)]
		public void  RefreshNamespaces ()
		{
			/*
		 * cache namespaces
		 */
			if (this.namespaceCache == null) {
				this.namespaceCache = new SortedDictionary<String, Namespace> ();
				foreach (Node node in client.Nodes) {
					try {
						String namespaceString = Info.Request (InfoPolicy, node, "namespaces");
						if (namespaceString.Trim ().Length > 0) {
							String[] namespaceList = namespaceString.Split (';');
							foreach (string nss in namespaceList) {
								Namespace ns = this.namespaceCache [nss];
								if (ns == null) {
									ns = new Namespace (nss);
									this.namespaceCache [nss] = ns;
								}
								RefreshNamespaceData (node, ns);
							}
						}
					} catch (AerospikeException e) {
						log.Error (String.Format ("Error geting Namespaces, Result code {0}, Message: {1} ", e.Message, e.Result));
					}	

				}
			}
		}

		public void RefreshNamespaceData (Node node, Namespace ns)
		{
			/*
		 * refresh namespace data
		 */
			try {
				String nameSpaceString = Info.Request (infoPolicy, node, "namespace/" + ns);
				ns.MergeNamespaceInfo (nameSpaceString);
				String setsString = Info.Request (infoPolicy, node, "sets/" + ns);
				if (setsString.Length > 0) {
					String[] sets = setsString.Split (';');
					foreach (String setData in sets) {
						ns.MergeSet (setData);
					}
				}
			} catch (AerospikeException e) {
				log.Error ("Error geting Namespace details", e);
			}	
		}

		public Namespace GetNamespace (String ns)
		{
			return namespaceCache [ns];
		}

		public List<Namespace> GetNamespaces ()
		{
			return namespaceCache.Values;
		}

		[MethodImpl (MethodImplOptions.Synchronized)]
		public void RefreshIndexes ()
		{
			/*
		 * cache index by Bin name
		 */
			if (this.indexCache == null)
				this.indexCache = new SortedDictionary<String, Index> ();

			foreach (Node node in client.Nodes) {
				if (node.Active) {
					try {
						String indexString = Info.Request (InfoPolicy, node, "sindex");
						if (indexString.Length > 0) {
							String[] indexList = indexString.Split (';');
							foreach (String oneIndexString in indexList) {
								Index index = new Index (oneIndexString);	
								String indexBin = index.Bin;
								this.indexCache [indexBin] = index;
							}
						}
						break;
					} catch (AerospikeException e) {
						log.Error ("Error geting Index informaton", e);
					}	
				}
			}
		}

		[MethodImpl (MethodImplOptions.Synchronized)]
		public Index getIndex (String binName)
		{
			return this.indexCache [binName];
		}

		[MethodImpl (MethodImplOptions.Synchronized)]
		public void RefreshModules ()
		{
			if (this.moduleCache == null)
				this.moduleCache = new SortedDictionary<String, Module> ();
			foreach (Node node in client.Nodes) {
				if (node.Active) {
					String packagesString = Info.Request (InfoPolicy, node, "udf-list");
					if (packagesString.Length > 0) {
						String[] packagesList = packagesString.Split (';');
						foreach (String pkgString in packagesList) {
							Module module = new Module (pkgString);
							String udfString = Info.Request (InfoPolicy, node, "udf-get:filename=" + module.Name);
							module.DetailInfo (udfString);//gen=qgmyp0d8hQNvJdnR42X3BXgUGPE=;type=LUA;recordContent=bG9jYWwgZnVuY3Rpb24gcHV0QmluKHIsbmFtZSx2YWx1ZSkKICAgIGlmIG5vdCBhZXJvc3Bpa2U6ZXhpc3RzKHIpIHRoZW4gYWVyb3NwaWtlOmNyZWF0ZShyKSBlbmQKICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgYWVyb3NwaWtlOnVwZGF0ZShyKQplbmQKCi0tIFNldCBhIHBhcnRpY3VsYXIgYmluCmZ1bmN0aW9uIHdyaXRlQmluKHIsbmFtZSx2YWx1ZSkKICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCmVuZAoKLS0gR2V0IGEgcGFydGljdWxhciBiaW4KZnVuY3Rpb24gcmVhZEJpbihyLG5hbWUpCiAgICByZXR1cm4gcltuYW1lXQplbmQKCi0tIFJldHVybiBnZW5lcmF0aW9uIGNvdW50IG9mIHJlY29yZApmdW5jdGlvbiBnZXRHZW5lcmF0aW9uKHIpCiAgICByZXR1cm4gcmVjb3JkLmdlbihyKQplbmQKCi0tIFVwZGF0ZSByZWNvcmQgb25seSBpZiBnZW4gaGFzbid0IGNoYW5nZWQKZnVuY3Rpb24gd3JpdGVJZkdlbmVyYXRpb25Ob3RDaGFuZ2VkKHIsbmFtZSx2YWx1ZSxnZW4pCiAgICBpZiByZWNvcmQuZ2VuKHIpID09IGdlbiB0aGVuCiAgICAgICAgcltuYW1lXSA9IHZhbHVlCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgZW5kCmVuZAoKLS0gU2V0IGEgcGFydGljdWxhciBiaW4gb25seSBpZiByZWNvcmQgZG9lcyBub3QgYWxyZWFkeSBleGlzdC4KZnVuY3Rpb24gd3JpdGVVbmlxdWUocixuYW1lLHZhbHVlKQogICAgaWYgbm90IGFlcm9zcGlrZTpleGlzdHMocikgdGhlbiAKICAgICAgICBhZXJvc3Bpa2U6Y3JlYXRlKHIpIAogICAgICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFZhbGlkYXRlIHZhbHVlIGJlZm9yZSB3cml0aW5nLgpmdW5jdGlvbiB3cml0ZVdpdGhWYWxpZGF0aW9uKHIsbmFtZSx2YWx1ZSkKICAgIGlmICh2YWx1ZSA+PSAxIGFuZCB2YWx1ZSA8PSAxMCkgdGhlbgogICAgICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCiAgICBlbHNlCiAgICAgICAgZXJyb3IoIjEwMDA6SW52YWxpZCB2YWx1ZSIpIAogICAgZW5kCmVuZAoKLS0gUmVjb3JkIGNvbnRhaW5zIHR3byBpbnRlZ2VyIGJpbnMsIG5hbWUxIGFuZCBuYW1lMi4KLS0gRm9yIG5hbWUxIGV2ZW4gaW50ZWdlcnMsIGFkZCB2YWx1ZSB0byBleGlzdGluZyBuYW1lMSBiaW4uCi0tIEZvciBuYW1lMSBpbnRlZ2VycyB3aXRoIGEgbXVsdGlwbGUgb2YgNSwgZGVsZXRlIG5hbWUyIGJpbi4KLS0gRm9yIG5hbWUxIGludGVnZXJzIHdpdGggYSBtdWx0aXBsZSBvZiA5LCBkZWxldGUgcmVjb3JkLiAKZnVuY3Rpb24gcHJvY2Vzc1JlY29yZChyLG5hbWUxLG5hbWUyLGFkZFZhbHVlKQogICAgbG9jYWwgdiA9IHJbbmFtZTFdCgogICAgaWYgKHYgJSA5ID09IDApIHRoZW4KICAgICAgICBhZXJvc3Bpa2U6cmVtb3ZlKHIpCiAgICAgICAgcmV0dXJuCiAgICBlbmQKCiAgICBpZiAodiAlIDUgPT0gMCkgdGhlbgogICAgICAgIHJbbmFtZTJdID0gbmlsCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgICAgIHJldHVybgogICAgZW5kCgogICAgaWYgKHYgJSAyID09IDApIHRoZW4KICAgICAgICByW25hbWUxXSA9IHYgKyBhZGRWYWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFNldCBleHBpcmF0aW9uIG9mIHJlY29yZAotLSBmdW5jdGlvbiBleHBpcmUocix0dGwpCi0tICAgIGlmIHJlY29yZC50dGwocikgPT0gZ2VuIHRoZW4KLS0gICAgICAgIHJbbmFtZV0gPSB2YWx1ZQotLSAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQotLSAgICBlbmQKLS0gZW5kCg==;
							this.moduleCache [module.Name] = module;
						}
					}
					break;
				}
			}
		}

		[MethodImpl (MethodImplOptions.Synchronized)]
		public Module GetModule (String moduleName)
		{
			return this.moduleCache [moduleName];
		}


		public void Close ()
		{
			if (this.client != null)
				this.client.Close ();
			indexCache.Clear ();
			indexCache = null;
			updatePolicy = null;
			insertPolicy = null;
			infoPolicy = null;
			moduleCache.Clear ();
			moduleCache = null;
		}

		public void Dispose ()
		{
			Close ();
		}
	}
}

