package com.aerospike.helper.query;

import com.aerospike.client.Value;
/**
 * Qualifier used to query by expiry epoch
 * @author peter
 *
 */
public class ExpiryQualifier extends Qualifier {
	public ExpiryQualifier(FilterOperation op, Value value) {
		super(QueryEngine.Meta.EXPIRATION.toString(), op, value);
	}
	@Override
	protected String luaFieldString(String field) {
		return "expiry";
	}
}