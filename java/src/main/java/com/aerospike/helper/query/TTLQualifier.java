package com.aerospike.helper.query;

import com.aerospike.client.Value;

public class TTLQualifier extends Qualifier {
	public TTLQualifier(Value value) {
		super(QueryEngine.Meta.TTL.toString(), FilterOperation.EQ, value);
	}
	@Override
	protected String luaFieldString(String field) {
		return "expiry";
	}

}
