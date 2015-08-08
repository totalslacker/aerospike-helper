package com.aerospike.helper.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;

public class Qualifier implements Map<String, Object>{
	private static final String FIELD = "field";
	private static final String VALUE2 = "value2";
	private static final String VALUE1 = "value1";
	private static final String OPERATION = "operation";
	private Map<String, Object> internalMap;
	public enum FilterOperation {
	    EQ, GT, GTEQ, LT, LTEQ, NOTEQ, BETWEEN, IN
	}
	
	public Qualifier() {
		super();
		internalMap = new HashMap<String, Object>();
	}
	public Qualifier(String field, FilterOperation operation, Value value1) {
		this();
		internalMap.put(FIELD, field);
		internalMap.put(OPERATION, operation);
		internalMap.put(VALUE1, value1);
	}
	public Qualifier(String field, FilterOperation operation, Value value1, Value value2) {
		this(field, operation, value1);
		internalMap.put(VALUE2, value2);
	}
	
	public FilterOperation getOperation(){
		return (FilterOperation) internalMap.get(OPERATION);
	}
	public String getField(){
		return (String) internalMap.get(FIELD);
	}
	public Value getValue1(){
		return (Value) internalMap.get(VALUE1);
	}
	public Value getValue2(){
		return (Value) internalMap.get(VALUE2);
	}

	public String luaFilterString(){

		StringBuilder res = new StringBuilder("rec['" + getField() + "']");
		FilterOperation op = getOperation();
		switch (op) {
		case EQ:
			res.append(" == " + luaValueString(getValue1()) );
			break;
		case NOTEQ:
			res.append("~= " + luaValueString(getValue1()) );
			break;
		case GT:
			res.append(" > " + luaValueString(getValue1()) );
			break;
		case GTEQ:
			res.append(" >= " + luaValueString(getValue1()) );
			break;
		case LT:
			res.append(" < " + luaValueString(getValue1()) );
			break;
		case LTEQ:
			res.append(" <= " + luaValueString(getValue1()) );
			break;
		case BETWEEN:
			res.append(" >= " + luaValueString(getValue1()) + " and rec['" + getField() + "'] <= " + luaValueString(getValue2()));			
			break;
		}
		return res.toString();
	}

	private String luaValueString(Value value){
		String res = null;
		int type = value.getType();
		switch (type) {
		case ParticleType.LIST:
			res = value.toString();
			break;
		case ParticleType.MAP:
			res = value.toString();
			break;
		case ParticleType.STRING:
			res = String.format("'%s'", value.toString());
			break;
		default:
			res = value.toString();
			break;
		}
		return res;
	}

	
	
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#size()
	 */
	@Override
	public int size() {
		return internalMap.size();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return internalMap.isEmpty();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(java.lang.Object key) {
		return internalMap.containsKey(key);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(java.lang.Object value) {
		return internalMap.containsValue(value);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#get(java.lang.Object)
	 */
	@Override
	public Object get(java.lang.Object key) {
		return internalMap.get(key);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Object put(String key, Object value) {
		return internalMap.put(key, value);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#remove(java.lang.Object)
	 */
	@Override
	public Object remove(java.lang.Object key) {
		return internalMap.remove(key);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		internalMap.putAll(m);
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear() {
		internalMap.clear();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#keySet()
	 */
	@Override
	public Set<String> keySet() {
		return internalMap.keySet();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#values()
	 */
	@Override
	public Collection<Object> values() {
		return internalMap.values();
	}
	/*
	 * (non-Javadoc)
	 * @see java.util.Map#entrySet()
	 */
	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return internalMap.entrySet();
	}


}
