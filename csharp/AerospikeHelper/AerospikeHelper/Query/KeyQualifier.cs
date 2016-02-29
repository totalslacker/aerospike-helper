using System;
using Aerospike.Helper.Model;
using Aerospike.Client;

namespace Aerospike.Helper.Query
{
	/// <summary>
	/// Qualifier used to query by primary key
	/// </summary>
	public class KeyQualifier : Qualifier
	{
		bool hasDigest = false;
		public KeyQualifier (Value value) : base(QueryEngine.Meta.KEY.ToString(), FilterOperation.EQ, value)
		{
		}
		public KeyQualifier(byte[] digest) : base(QueryEngine.Meta.KEY.ToString(), FilterOperation.EQ, null){
			
			this.internalMap["digest"] = digest;
			this.hasDigest = true;
		}

		protected override String luaFieldString(String field) {
			return "digest";
		}

		public byte[] Digest{
			get {
			return (byte[]) this.internalMap["digest"];
			}
		}

		public Key MakeKey(String ns, String set){
			if (hasDigest){
				return new Key(ns, Digest, null, null);
			} else {
				return new Key(ns, set, Value1);
			}

		}
	}
}

