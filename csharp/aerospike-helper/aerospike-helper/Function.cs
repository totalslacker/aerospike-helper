using System;

namespace aerospikehelper
{
	public class Function
	{
		private String name;

		public Function(Module parent, String name) {
			this.name = name;
		}
		public String Name
		{
			get
			{
				return this.name;
			}
		}

		public override String ToString() 
		{
			return this.name;
		}
	}
}

