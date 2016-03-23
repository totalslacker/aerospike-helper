using System;

namespace Aerospike.Helper
{
	public class NotImplementedException : Exception
	{
		public NotImplementedException () : base("Method not implemented")
		{
			
		}
		public NotImplementedException(string message)
			: base(message)
		{
		}

		public NotImplementedException(string message, Exception inner)
			: base(message, inner)
		{
		}
	}
}

