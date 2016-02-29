using System;
using Aerospike.Client;

namespace Aerospike.Helper.Query
{
	public class ExpiryQualifer : Qualifier
	{
		public ExpiryQualifer (FilterOperation op, Value value) : base(QueryEngine.Meta.EXPIRATION.ToString(), op, value){
		}

		protected override String luaFieldString(String field) {
			return "expiry";
		}
	}
}

