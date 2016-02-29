using System;
using System.Collections.Generic;
using System.Collections;

namespace Aerospike.Helper.Model
{
	public class Namespace
	{
		protected String name;
		protected Dictionary<String, Set> sets;
		protected Dictionary<String, NameValuePair> values;
		protected HashSet<String> dontMerge = new HashSet<String>
		{
			"available-bin-names", 
			"cold-start-evict-ttl", 
			"current-time",
			"default-ttl",
			"evict-tenths-pct",
			"free-pct-memory",
			"high-water-disk-pct",
			"high-water-memory-pct",
			"max-ttl",
			"max-void-time",
			"nsup-cycle-duration",
			"nsup-cycle-sleep-pct",
			"repl-factor",
			"stop-writes-pct"
		};

		public Namespace(String name) {
			this.name = name;
			values = new Dictionary<String, NameValuePair>();
		}

		public override String ToString() {
			return this.name;
		}

		public override bool Equals(Object obj) {
			return ((obj is Namespace) &&
				(obj.ToString().Equals(ToString())));
		}

		public override int GetHashCode ()
		{
			int hash = 13;
			hash = (hash * 7) + name.GetHashCode();
			return hash;
		}


		public void AddSet(String setData){
			if (sets == null)
				sets = new Dictionary<String, Set>();
			Set newSet = new Set(this, setData);
			Set existingSet = sets[newSet.Name];
			if (existingSet == null){
				sets.Add(newSet.Name, newSet);
			} else {
				existingSet.Info(setData);
			}
		}

		public void MergeSet(String setData){
			if (sets == null)
				sets = new Dictionary<String, Set>();
			Set newSet = new Set(this, setData);
			Set existingSet = sets[newSet.Name];
			if (existingSet == null){
				sets.Add(newSet.Name, newSet);
			} else {
				existingSet.MergeSetInfo(setData);
			}
		}

		public Dictionary<String, Set> Sets() {
			if (sets == null)
				sets = new Dictionary<String, Set>();
			return sets;
		}

		public String Name() {
			return ToString();
		}

		public void Clear(){
			if (this.sets != null){
				this.sets.Clear();
				}

		}
		//	public void setValues(Map<String, NameValuePair> newValues){
		//		this.values = newValues;
		//	}

		public Dictionary<String, NameValuePair> Values
		{
			get
			{
				return values;
			}
		}

		public void Info(String info, Dictionary<String, NameValuePair> map, bool merge) {
			/*
		type=device;objects=0;master-objects=0;prole-objects=0;expired-objects=0;evicted-objects=0; \
		set-deleted-objects=0;set-evicted-objects=0;used-bytes-memory=18688;data-used-bytes-memory=0; \
		index-used-bytes-memory=0;sindex-used-bytes-memory=18688;free-pct-memory=99;max-void-time=0; \
		min-evicted-ttl=0;max-evicted-ttl=0;non-expirable-objects=0;current-time=137899728; \
		stop-writes=false;hwm-breached=false;available-bin-names=32767;ldt_reads=0;ldt_read_success=0; \
		ldt_deletes=0;ldt_delete_success=0;ldt_writes=0;ldt_write_success=0;ldt_updates=0;ldt_errors=0; \
		used-bytes-disk=0;free-pct-disk=100;available_pct=99;sets-enable-xdr=true;memory-size=4294967296; \
		low-water-pct=0;high-water-disk-pct=50;high-water-memory-pct=60;evict-tenths-pct=5; \
		stop-writes-pct=90;cold-start-evict-ttl=4294967295;repl-factor=1;default-ttl=2592000;max-ttl=0; \
		conflict-resolution-policy=generation;allow_versions=false;single-bin=false;enable-xdr=false; \
		disallow-null-setname=false;total-bytes-memory=4294967296;total-bytes-disk=4294967296; \
		defrag-period=10;defrag-max-blocks=4000;defrag-lwm-pct=45;write-smoothing-period=0; \
		defrag-startup-minimum=10;max-write-cache=67108864;min-avail-pct=5;post-write-queue=0; \
		data-in-memory=true;load-at-startup=true;file=/opt/aerospike/test.data;filesize=4294967296; \
		writethreads=1;writecache=67108864;obj-size-hist-max=100
		 */
			if (map == null)
				return;

			if (info.Length == 0)
				return;
			String[] parts = info.Split(';');

			foreach (String part in parts){
				String[] kv = part.Split('=');
				String key = kv[0];
				String value = kv[1];
				NameValuePair storedValue = map[key];
				if (storedValue == null){
					storedValue = new NameValuePair(this, key, value);
					map[key] = storedValue;
				} else {
					if (merge && !dontMerge.Contains(key)){
						try{
							long newValue = Int64.Parse(value);
							long oldValue = Int64.Parse(storedValue.value.ToString());
							storedValue.value = (oldValue + newValue);
						} catch (FormatException e){
							storedValue.value = value;
						}
					} else {
						storedValue.value = value;
					}
				}
			}
		}
		public void NamespaceInfo(String info) {
			Info(info, values, false);

		}
		public void  MergeNamespaceInfo(String info){
			Info(info, values, true);
		}

		public Set FindSet(String tableName) {
			Set result = null;
			this.sets.TryGetValue (tableName, out result);
				return result;
		}
	}
}

