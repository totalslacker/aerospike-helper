using System;
using log4net;
using Aerospike.Client;
using System.Collections.Generic;
using AerospikeHelper.Model;
using System.Runtime.CompilerServices;

namespace AerospikeHelper
{
	/// <summary>
	/// Iterator for traversing a collection of KeyRecords
	/// </summary>
	public class KeyRecordIterator : System.Collections.IEnumerator, IDisposable
	{
		private const String META_DATA = "meta_data";
		private const String SET_NAME = "set_name";
		private const String DIGEST = "digest";
		private const String EXPIRY = "expiry";
		private const String GENERATION = "generation";
		private static readonly ILog log = LogManager.GetLogger (typeof(KeyRecordIterator));
		private RecordSet recordSet;
		private ResultSet resultSet;
		private String ns;
		private KeyRecord singleRecord;

		public KeyRecordIterator (String ns)
		{
			this.ns = ns;
		}

		public KeyRecordIterator(String ns, RecordSet recordSet) : base(ns){
			
			this.recordSet = recordSet;

		}


		public KeyRecordIterator(String ns, ResultSet resultSet) : base(ns){
			this.resultSet = resultSet;

		}

		public void Dispose ()
		{
			Close ();
		}

		[MethodImpl (MethodImplOptions.Synchronized)]
		public void Close() {
				if (recordSet != null)
					recordSet.Close();
				if (resultSet != null)
					resultSet.Close();
				if (singleRecord != null)
					singleRecord = null;
		}

		public System.Collections.IEnumerator GetEnumerator(){
		//public KeyRecord Next() {
			KeyRecord keyRecord = null;

			if (this.recordSet != null && this.recordSet.Next()) {
				keyRecord = new KeyRecord(this.recordSet.Key, this.recordSet.Record);

			} else if (this.resultSet != null) {
				Dictionary<String, Object> map = (Dictionary<String, Object>) this.resultSet.Next();
				Dictionary<String,Object> meta = (Dictionary<String, Object>) map[META_DATA];
				map.Remove(META_DATA);
				Dictionary<String,Object> binMap = new Dictionary<String, Object>(map);
				if (log.IsDebugEnabled){
//					for (Map.Entry<String, Object> entry : map.entrySet())
//					{
//						log.Debug(entry.getKey() + " = " + entry.getValue());
//					}
				}
				long generation =  (long) meta[GENERATION];
				long ttl =  (long) meta[EXPIRY];
				Record record = new Record(binMap, generation, ttl);
				Key key = new Key(ns, (byte[]) meta[DIGEST], (String) meta[SET_NAME], null);
				keyRecord = new KeyRecord(key , record);

			} else if (singleRecord != null){
				keyRecord = singleRecord;
				singleRecord = null;
			}
			yield return keyRecord;
		}


		public override String ToString() {
			return this.ns;
		}

	}
}

