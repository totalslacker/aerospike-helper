package com.aerospike.helper.query;

import com.aerospike.client.Value;

public class ExipryQualifier extends Qualifier {
	public ExipryQualifier(FilterOperation op, Value value) {
		super(QueryEngine.Meta.EXPIRATION.toString(), op, value);
	}
	@Override
	protected String luaFieldString(String field) {
		return "expiry";
	}
}
