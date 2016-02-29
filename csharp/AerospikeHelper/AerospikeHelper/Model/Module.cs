using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Collections;

namespace Aerospike.Helper.Model
{
	public class Module
	{
		private String name;
		protected Dictionary<String, String> values;
		private String source;
		private List<Function> functions;

		private Regex fn_name = new Regex("function\\s+([a-zA-Z_$][a-zA-Z\\d_$]*\\(.*\\))");

		public Module(String info) {
			PackageInfo(info);

		}

		public override int GetHashCode ()
		{
			int hash = 13;
			hash = (hash * 7) + name.GetHashCode();
			hash = (hash * 7) + source.GetHashCode();
			return hash;
		}

		public override String ToString() {
			return this.Name;
		}

		public String Name
		{
			get
			{
				return this.name;
			}
		}

		public override bool Equals(System.Object obj)
		{
			return ((obj is Module) &&
				(obj.ToString().Equals(ToString())));
		}

		public static String NameFromInfo(String info){
			//filename=a_test_udf.lua,hash=874473d6583f6c4d16ce5ff3e14f2dca75bee062,type=LUA
			if (info.Length > 0)
			{
				String[] parts = info.Split(',');
				return parts[0].Substring(9, parts[0].Length-1);
			}
			return null;
		}

		public void PackageInfo(String info){
			//filename=a_test_udf.lua,hash=874473d6583f6c4d16ce5ff3e14f2dca75bee062,type=LUA
			if (info.Length > 0){
				String[] parts = info.Split(',');
				if (values == null){
					values = new Dictionary<String, String>();
				}
				foreach (String part in parts){
					KvPut(part, this.values);
				}
				this.name = values["filename"];
			}
		}

		private void KvPut(String kv, Dictionary<String, String> map){
			String[] kvParts = kv.Split('=');
			map.Add(kvParts[0], kvParts[1]);
		}

		public void DetailInfo(String info){
			//gen=qgmyp0d8hQNvJdnR42X3BXgUGPE=;type=LUA;recordContent=bG9jYWwgZnVuY3Rpb24gcHV0QmluKHIsbmFtZSx2YWx1ZSkKICAgIGlmIG5vdCBhZXJvc3Bpa2U6ZXhpc3RzKHIpIHRoZW4gYWVyb3NwaWtlOmNyZWF0ZShyKSBlbmQKICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgYWVyb3NwaWtlOnVwZGF0ZShyKQplbmQKCi0tIFNldCBhIHBhcnRpY3VsYXIgYmluCmZ1bmN0aW9uIHdyaXRlQmluKHIsbmFtZSx2YWx1ZSkKICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCmVuZAoKLS0gR2V0IGEgcGFydGljdWxhciBiaW4KZnVuY3Rpb24gcmVhZEJpbihyLG5hbWUpCiAgICByZXR1cm4gcltuYW1lXQplbmQKCi0tIFJldHVybiBnZW5lcmF0aW9uIGNvdW50IG9mIHJlY29yZApmdW5jdGlvbiBnZXRHZW5lcmF0aW9uKHIpCiAgICByZXR1cm4gcmVjb3JkLmdlbihyKQplbmQKCi0tIFVwZGF0ZSByZWNvcmQgb25seSBpZiBnZW4gaGFzbid0IGNoYW5nZWQKZnVuY3Rpb24gd3JpdGVJZkdlbmVyYXRpb25Ob3RDaGFuZ2VkKHIsbmFtZSx2YWx1ZSxnZW4pCiAgICBpZiByZWNvcmQuZ2VuKHIpID09IGdlbiB0aGVuCiAgICAgICAgcltuYW1lXSA9IHZhbHVlCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgZW5kCmVuZAoKLS0gU2V0IGEgcGFydGljdWxhciBiaW4gb25seSBpZiByZWNvcmQgZG9lcyBub3QgYWxyZWFkeSBleGlzdC4KZnVuY3Rpb24gd3JpdGVVbmlxdWUocixuYW1lLHZhbHVlKQogICAgaWYgbm90IGFlcm9zcGlrZTpleGlzdHMocikgdGhlbiAKICAgICAgICBhZXJvc3Bpa2U6Y3JlYXRlKHIpIAogICAgICAgIHJbbmFtZV0gPSB2YWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFZhbGlkYXRlIHZhbHVlIGJlZm9yZSB3cml0aW5nLgpmdW5jdGlvbiB3cml0ZVdpdGhWYWxpZGF0aW9uKHIsbmFtZSx2YWx1ZSkKICAgIGlmICh2YWx1ZSA+PSAxIGFuZCB2YWx1ZSA8PSAxMCkgdGhlbgogICAgICAgIHB1dEJpbihyLG5hbWUsdmFsdWUpCiAgICBlbHNlCiAgICAgICAgZXJyb3IoIjEwMDA6SW52YWxpZCB2YWx1ZSIpIAogICAgZW5kCmVuZAoKLS0gUmVjb3JkIGNvbnRhaW5zIHR3byBpbnRlZ2VyIGJpbnMsIG5hbWUxIGFuZCBuYW1lMi4KLS0gRm9yIG5hbWUxIGV2ZW4gaW50ZWdlcnMsIGFkZCB2YWx1ZSB0byBleGlzdGluZyBuYW1lMSBiaW4uCi0tIEZvciBuYW1lMSBpbnRlZ2VycyB3aXRoIGEgbXVsdGlwbGUgb2YgNSwgZGVsZXRlIG5hbWUyIGJpbi4KLS0gRm9yIG5hbWUxIGludGVnZXJzIHdpdGggYSBtdWx0aXBsZSBvZiA5LCBkZWxldGUgcmVjb3JkLiAKZnVuY3Rpb24gcHJvY2Vzc1JlY29yZChyLG5hbWUxLG5hbWUyLGFkZFZhbHVlKQogICAgbG9jYWwgdiA9IHJbbmFtZTFdCgogICAgaWYgKHYgJSA5ID09IDApIHRoZW4KICAgICAgICBhZXJvc3Bpa2U6cmVtb3ZlKHIpCiAgICAgICAgcmV0dXJuCiAgICBlbmQKCiAgICBpZiAodiAlIDUgPT0gMCkgdGhlbgogICAgICAgIHJbbmFtZTJdID0gbmlsCiAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQogICAgICAgIHJldHVybgogICAgZW5kCgogICAgaWYgKHYgJSAyID09IDApIHRoZW4KICAgICAgICByW25hbWUxXSA9IHYgKyBhZGRWYWx1ZQogICAgICAgIGFlcm9zcGlrZTp1cGRhdGUocikKICAgIGVuZAplbmQKCi0tIFNldCBleHBpcmF0aW9uIG9mIHJlY29yZAotLSBmdW5jdGlvbiBleHBpcmUocix0dGwpCi0tICAgIGlmIHJlY29yZC50dGwocikgPT0gZ2VuIHRoZW4KLS0gICAgICAgIHJbbmFtZV0gPSB2YWx1ZQotLSAgICAgICAgYWVyb3NwaWtlOnVwZGF0ZShyKQotLSAgICBlbmQKLS0gZW5kCg==;
			String[] udfparts = info.Split(';');
			foreach (String kv in udfparts){
				KvPut(kv, this.values);
			}
				String code = values["recordContent"];
				//code = code.substring(0, code.length()-2);
				if (code != null)
				Source = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(code));

		}
		public void SetValues(Dictionary<String, String> newValues)
		{
			this.values = newValues;
		}
			
		public List<NameValuePair> GetValues()
		{
			List<NameValuePair> result = new List<NameValuePair>();
			foreach(var item in  this.values)
			{
				String value = this.values[item.Key];
				NameValuePair nvp = new  NameValuePair(this, item.Key, value);
				result.Add(nvp);
			}
			return result;
		}

		public String Source
		{
			get
			{
				return source;
			}
			set
			{
				this.source = Source;
				//Match matcher = fn_name.Match(source);
				functions = new List<Function>();

				foreach (Match functionMatch in fn_name.Matches(source))
				{
					String functionString = functionMatch.Groups[1].Value;
					functions.Add(new Function(this, functionString));
				}

			}
		}
	}
}

