using System;

namespace Aerospike.Helper.Model
{
	public class NameValuePair
	{
		public String name;
		public Object value;
		public NameValuePair (Object parent, String name, Object value)
		{
			this.name = name;
			this.value = value;
		}
		public String Name
		{
			get
			{
				return this.name;
			}
		}
		public Object Value
		{
			get
			{
				return this.value;
			}
		}

		public override String ToString() 
		{
			return name + "|" + value.ToString();
		}

		public void Clear(){
			if (this.value != null && (this.value is long)){
				this.value = 0L;
			} else if (this.value != null && (this.value is String)){
				this.value = "";
			} else if (this.value != null && (this.value is int)){
				this.value = 0;
			} else if (this.value != null && (this.value is float)){
				this.value = 0.0;
			}
		}

	}
}

