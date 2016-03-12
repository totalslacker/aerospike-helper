using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Helper.Model
{
	public class Index
	{
		protected Dictionary<string, string> values;
		public Index (String info)
		{
			Info (info);
		}
		public String Name
		{
			get
			{
				return values["indexname"];
			}
		}
		public Dictionary<string, string> Values
		{
			get
			{
				return values;
			}
		}
		public void Info(String info){
			//ns=phobos_sindex:set=longevity:indexname=str_100_idx:num_bins=1:bins=str_100_bin:type=TEXT:sync_state=synced:state=RW;
			//ns=test:set=Customers:indexname=mail_index_userss:bin=email:type=STRING:indextype=LIST:path=email:sync_state=synced:state=RW
			if (info.Length > 0){
				String[] parts = info.Split(':');
				foreach (String part in parts){
					KvPut(part);
				}
			}
		}
		private void KvPut(String kv){
			if (values == null){
				values = new Dictionary<string, string>();
			}
			String[] kvParts = kv.Split('=');
			this.values[kvParts[0]] = kvParts[1];
		}
		public override String ToString() {
			return this.Name;
		}
		public String Bin 
		{
			get
			{
				String binName = "";
				if (values != null && values.TryGetValue("bin", out binName))
					return 	binName;
				else
					return null;
			}
		}
		public IndexType Type 
		{
			get 
			{
				String indexTypeString = values["type"];
				if (indexTypeString.Equals("TEXT"))
					return IndexType.STRING;
				else
					return IndexType.NUMERIC;
			}
		}


	}
}

