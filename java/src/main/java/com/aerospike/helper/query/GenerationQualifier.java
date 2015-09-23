package com.aerospike.helper.query;

import com.aerospike.client.Value;
/**
 * Qualifier used to query by generation
 * @author peter
 *
 */
public class GenerationQualifier extends Qualifier {

	public GenerationQualifier(FilterOperation op, Value value) {
		super(QueryEngine.Meta.GENERATION.toString(), op, value);
	}
	@Override
	protected String luaFieldString(String field) {
		return "generation";
	}

}