package com.aerospike.helper.query;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;

public class KeyRecordIterator implements Iterator<KeyRecord>, Closeable {
	private static Logger log = Logger.getLogger(KeyRecordIterator.class);
	private RecordSet recordSet;
	private ResultSet resultSet;
	private Iterator<KeyRecord> recordSetIterator;
	private Iterator<Object> resultSetIterator;
	private String namespace;
	
	public KeyRecordIterator(String namespace) {
		super();
		this.namespace = namespace;
	}

	public KeyRecordIterator(String namespace, RecordSet recordSet) {
		this(namespace);
		this.recordSet = recordSet;
		this.recordSetIterator = recordSet.iterator();
	}


	public KeyRecordIterator(String namespace, ResultSet resultSet) {
		this(namespace);
		this.resultSet = resultSet;
		this.resultSetIterator = resultSet.iterator();
		
	}

	@Override
	public void close() throws IOException {
		if (recordSet != null)
			recordSet.close();
		if (resultSet != null)
			resultSet.close();
	}

	@Override
	public boolean hasNext() {
		if (this.recordSetIterator != null)
			return this.recordSetIterator.hasNext();
		else
			return this.resultSetIterator.hasNext();
	}

	@Override
	public KeyRecord next() {
		KeyRecord keyRecord = null;

		if (this.recordSetIterator != null) {
			keyRecord = this.recordSetIterator.next();
		} else {
			Map<String, Object> map = (Map) this.resultSetIterator.next();
			Map<String,Object> meta = (Map<String, Object>) map.get("meta_data");
			map.remove("meta_data");
			Map<String,Object> binMap = new HashMap<String, Object>(map);
			if (log.isDebugEnabled()){
				for (Map.Entry<String, Object> entry : map.entrySet())
				{
					log.debug(entry.getKey() + " = " + entry.getValue());
				}
			}
			Long generation =  (Long) meta.get("generation");
			Long ttl =  (Long) meta.get("ttl");
			Record record = new Record(binMap, generation.intValue(), ttl.intValue());
			Key key = new Key(namespace, (String) meta.get("set_name"), (byte[]) meta.get("digest")); 
			keyRecord = new KeyRecord(key , record);
		}
		return keyRecord;
	}

	@Override
	public void remove() {
		
	}

}
