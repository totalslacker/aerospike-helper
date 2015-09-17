package com.aerospike.helper.query;

import com.aerospike.client.Value;

public class KeyQualifier extends Qualifier {

	public KeyQualifier(Value value) {
		super(QueryEngine.Meta.KEY.toString(), FilterOperation.EQ, value);
	}
	@Override
	protected String luaFieldString(String field) {
		return "digest";
	}

}
