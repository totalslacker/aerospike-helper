using System;
using Aerospike.Client;

namespace Aerospike.Helper.Query
{	
	/// <summary>
	/// Qualifier used to query by generation
	/// </summary>
	public class GenerationQualifier : Qualifier
	{
		public GenerationQualifier(FilterOperation op, Value value): base(QueryEngine.Meta.GENERATION.ToString(), op, value){
		}

		protected override string luaFieldString(String field) {
			return "generation";
		}


	}
}

