using System;
using log4net;
using Aerospike.Client;
using System.Collections.Generic;
using Aerospike.Helper.Model;
using System.Runtime.CompilerServices;

namespace Aerospike.Helper.Query
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
		private KeyRecord currentRecord;

		public KeyRecordIterator (String ns)
		{
			this.ns = ns;
		}

		public KeyRecordIterator(String ns, KeyRecord singleRecord) : this(ns){

			this.singleRecord = singleRecord;

		}
		public KeyRecordIterator(String ns, RecordSet recordSet) : this(ns){
			
			this.recordSet = recordSet;

		}


		public KeyRecordIterator(String ns, ResultSet resultSet) : this(ns){
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
				singleRecord = null;
		}

		public bool MoveNext(){
			bool hasNext = false;
			if (this.recordSet != null && this.recordSet.Next()) {
				currentRecord = new KeyRecord(this.recordSet.Key, this.recordSet.Record);
				hasNext = true;

			} else if (this.resultSet != null && this.resultSet.Next()) {
				Dictionary<String, Object> map = (Dictionary<String, Object>) this.resultSet.Object;
				Dictionary<String,Object> meta = (Dictionary<String, Object>) map[META_DATA];
				map.Remove(META_DATA);
				Dictionary<String,Object> binMap = new Dictionary<String, Object>(map);
				if (log.IsDebugEnabled){
					//					for (Map.Entry<String, Object> entry : map.entrySet())
					//					{
					//						log.Debug(entry.getKey() + " = " + entry.getValue());
					//					}
				}
				int generation =  (int) meta[GENERATION];
				int ttl =  (int) meta[EXPIRY];
				Record record = new Record(binMap, generation, ttl);
				Key key = new Key(ns, (byte[]) meta[DIGEST], (String) meta[SET_NAME], null);
				currentRecord = new KeyRecord(key , record);
				hasNext = true;

			} else if (singleRecord != null){
				currentRecord = singleRecord;
				singleRecord = null;
				hasNext = true;
			}

			return hasNext;		
		}

		public void Reset(){
		}

		public Object Current
		{
			get { return currentRecord; }
		}

		public System.Collections.IEnumerator GetEnumerator(){
		
			return null; // dont know what to do here

		}


		public override String ToString() {
			return this.ns;
		}

	}
}

