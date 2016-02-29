package com.aerospike.helper.collections;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.WritePolicy;
/**
 * An implementation of LargeList using standard KV operations
 * 
 * WARNING: This code is only a first cut and not extensively tested
 * @author peter
 *
 */
public class LargeList {
	public static final String ListElementBinName = "__ListElement";

	private AerospikeClient client;
	private WritePolicy policy;
	private Key key;
	private Value binName;
	private String binNameString;

	public LargeList(AerospikeClient client, WritePolicy policy, Key key, String binName)
	{
		this.client = client;
		this.policy = policy;
		this.key = key;
		this.binName = Value.get(binName);
		this.binNameString = this.binName.toString ();
	}

	private List<byte[]> subRecordList(Key key){
		Record record = client.get (this.policy, key, binNameString);
		if (record != null) {
			return (List<byte[]>)record.getValue (binNameString);
		}
		return null;
	}

	private Key makeSubKey(Value value) {
		Key subKey;
		String valueString;
		if (value instanceof Value.MapValue) {

			Map map = (Map) value.getObject();
			valueString = map.get("key").toString();

		} else {

			valueString = value.toString ();

		}
		String subKeyString = String.format ("%s::%s", this.key.userKey.toString (), valueString);
		subKey = new Key (this.key.namespace, this.key.setName, subKeyString);
		return subKey;
	}

	private Key[] makeSubKeys(List<Value> values) {
		Key[] keys = new Key[values.size()];
		int index = 0;
		for (Value value : values) {
			keys [index] = makeSubKey (value);
			index++;
		}
		return keys;
	}

	private List<byte[]> getDigestList(){
		Record topRecord = client.get (this.policy, this.key, this.binNameString);
		if (topRecord == null)
			return new ArrayList<byte[]> ();
			List<byte[]> digestList = (List<byte[]>) topRecord.getValue (this.binNameString);
			if (digestList == null)
				return new ArrayList<byte[]> ();
				return digestList;
	}

	public void add(Value value)
	{

		Key subKey = makeSubKey (value);

		client.put (this.policy, subKey, new Bin (ListElementBinName, value));

		// add the digest of the subKey to the CDT List in the Customer record
		client.operate(this.policy, this.key, ListOperation.append(this.binNameString, Value.get(subKey.digest)));

	}
	public void add(List<Value> items)
	{
		for(Value Value : items){
			this.add(Value);
		}
	}

	public void add(Value... items)
	{
		for(Value Value : items){
			this.add(Value);
		}
	}
	public void update(Value value)
	{

		if (size () == 0) {
			add (value);
		} else {
			Key subKey = makeSubKey (value);
			client.put (this.policy, subKey, new Bin (ListElementBinName, value));
		}
	}
	public void update(Value... values)
	{
		for(Value value : values){
			this.update(value);
		}
	}

	public void update(List<Value> values)
	{
		for(Value value : values){
			this.update(value);
		}

	}

	public void remove(Value value)
	{
		Key subKey = makeSubKey (value);
		List<byte[]> digestList = getDigestList ();
		int index = digestList.indexOf (subKey.digest);
		client.delete (this.policy, subKey);
		client.operate(this.policy, this.key, ListOperation.remove(this.binNameString, index));

	}

	public void remove(List<Value> values)
	{
		Key[] keys = makeSubKeys (values);
		List<byte[]> digestList = getDigestList ();

		//		int startIndex = digestList.IndexOf (subKey.digest);
		//		int count = values.Count;
		//		foreach (Key key in keys){
		//			
		//			client.Delete (this.policy, key);
		//		}
		//		client.Operate(this.policy, this.key, ListOperation.Remove(this.binNameString, startIndex, count));

		for (Key key : keys){

			client.delete (this.policy, key);
			digestList.remove (key.digest);

		}

		client.put (this.policy, this.key, new Bin (this.binNameString, digestList));

	}

	public int remove(Value begin, Value end)
	{
		List<byte[]> digestList = getDigestList ();
		Key beginKey = makeSubKey (begin);
		Key endKey = makeSubKey (end);
		int start = digestList.indexOf (beginKey.digest);
		int stop = digestList.indexOf (endKey.digest);
		int count = stop - start + 1;;
		for (int i = start; i < stop; i++){
			Key subKey = new Key (this.key.namespace, (byte[])digestList.get(i), null, null);
			client.delete (this.policy, subKey);
		}
		client.operate(this.policy, this.key, ListOperation.removeRange(this.binNameString, start, count));
		return count;
	}
	public boolean exists(Value keyValue)
	{
		Key subKey = makeSubKey (keyValue);
		return client.exists (this.policy, subKey);

	}

	public List<Boolean> exists (List<Value> keyValues)
	{
		List<Boolean> target = new ArrayList<Boolean> ();
		for (Object value : keyValues) {
			target.add (exists (Value.get(value)));
		}
		return target;

	}

	public List<?> find(Value value)
	{
		Key subKey = makeSubKey (value);
		Record record = client.get (this.policy, subKey, ListElementBinName);
		if (record != null) {
			final Object result = record.getValue (ListElementBinName);
			return new ArrayList<Object> (){{ add(result); }};
		} else {
			return null;
		}

	}

	private List<?> get(List<byte[]> digestList, int start, int stop){
		List<Object> results = new ArrayList<Object> ();

		for (int i = start; i < stop; i++) {
			Key subKey = new Key (this.key.namespace, (byte[])digestList.get(i), null, null);
			Record record = client.get (this.policy, subKey, ListElementBinName);
			Object result = record.getValue (ListElementBinName);
			results.add (result);
		}
		return results;
	}

	public List<?> findFrom (Value begin, int count)
	{
		List<byte[]> digestList = getDigestList ();
		Key beginKey = makeSubKey (begin);
		int start = digestList.indexOf (beginKey.digest);
		int stop = start + count;
		return get (digestList, start, stop);
	}
	//
	//	public IList FindFrom(Value begin, int count, string filterModule, string filterName, params Value[] filterArgs)
	//	{
	//		return (IList)client.Execute(policy, key, PackageName, "find_from", binName, begin, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
	//	}

	public List<?> range(Value begin, Value end)
	{
		List<byte[]> digestList = getDigestList ();
		Key beginKey = makeSubKey (begin);
		Key endKey = makeSubKey (end);
		int start = digestList.indexOf (beginKey.digest);
		int stop = digestList.indexOf (endKey.digest);
		return get (digestList, start, stop);
	}

	//	public IList Range(Value begin, Value end, int count)
	//	{
	//		return (IList)client.Execute(policy, key, PackageName, "find_range", binName, begin, end, Value.Get(count));
	//	}
	//
	//	public IList Range(Value begin, Value end, string filterModule, string filterName, params Value[] filterArgs)
	//	{
	//		return (IList)client.Execute(policy, key, PackageName, "range", binName, begin, end, Value.Get(filterModule), Value.Get(filterModule), Value.Get(filterArgs));
	//	}
	//
	//	public IList Range(Value begin, Value end, int count, string filterModule, string filterName, params Value[] filterArgs)
	//	{
	//		return (IList)client.Execute(policy, key, PackageName, "find_range", binName, begin, end, Value.Get(count), Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
	//	}

	public List<?> scan()
	{

		List<byte[]> digestList = getDigestList ();
		if (digestList != null || digestList.size() > 0) {
			List<Value> results = new ArrayList<Value> ();
			for (byte[] digest : digestList) {
				Key subKey = new Key (this.key.namespace, digest, null, null);
				Record record = client.get (this.policy, subKey, this.binNameString);
				results.add ((Value) record.getValue (ListElementBinName));
			}
			return results;

		}
		return null;
	}

	//	public IList Filter(string filterModule, string filterName, params Value[] filterArgs)
	//	{
	//		return (IList)client.Execute(policy, key, PackageName, "filter", binName, Value.AsNull, Value.Get(filterModule), Value.Get(filterName), Value.Get(filterArgs));
	//	}

	public void destroy ()
	{
		List<byte[]> digestList = getDigestList ();

		client.put (this.policy, this.key, Bin.asNull(this.binNameString));

		for (byte[] digest : digestList) {
			Key subKey = new Key (this.key.namespace, digest, null, null);
			client.delete (this.policy, subKey);
		}


	}

	public int size()
	{
		Record record = client.operate(this.policy, this.key, ListOperation.size(this.binNameString));
		return record.getInt ("Count");
	}

	public Map<?,?> GetConfig()
	{
		return null;  // No config because its NotSupportedException AsyncNode LDT
	}

	public void SetPageSize(int pageSize)
	{
		// do nothing
	}

}

