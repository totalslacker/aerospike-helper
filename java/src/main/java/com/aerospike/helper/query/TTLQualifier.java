package com.aerospike.helper.query;

import com.aerospike.client.Value;
/**
 * Time to live qualifier - not necessary
 * @author peter
 *
 */
@Deprecated
public class TTLQualifier extends Qualifier {
	public TTLQualifier(Value value) {
		super(QueryEngine.Meta.TTL.toString(), FilterOperation.EQ, value);
	}
	@Override
	protected String luaFieldString(String field) {
		return "expiry";
	}

}
