package com.aerospike.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.helper.model.NameValuePair;

public class Utils {
	
	public static void printInfo(String title, String infoString){
		if (infoString == null){
			System.out.println("Null info string");
			return;
		}
		String[] outerParts = infoString.split(";");
		System.out.println(title);
		for (String s : outerParts){

			String[] innerParts = s.split(":");
			for (String parts : innerParts){
				System.out.println("\t" + parts);
			}
			System.out.println();
		}

	}
	public static  String infoAll(AerospikeClient client, String cmd) throws AerospikeException{
		Node[] nodes = client.getNodes();
		StringBuilder results = new StringBuilder();
		for (Node node : nodes){
			results.append(Info.request(node.getHost().name, node.getHost().port, cmd)).append("\n");
		}
		return results.toString();
	}
	public static Map<String, String> toMap(String source){
		HashMap<String, String> responses = new HashMap<String, String>();
		String values[] = source.split(";");

		for (String value : values) {
			String nv[] = value.split("=");

			if (nv.length >= 2) {
				responses.put(nv[0], nv[1]);
			}
			else if (nv.length == 1) {
				responses.put(nv[0], null);
			}
		}

		return responses.size() != 0 ? responses : null;

	}
	
	public static List<NameValuePair> toNameValuePair(Object parent, Map<String, String> map){
		List<NameValuePair> list = new ArrayList<NameValuePair>();
		for (String key : map.keySet()){
			NameValuePair nvp = new NameValuePair(parent, key, map.get(key));
			list.add(nvp);
		}
		return list;
	}

	
	public static <T> T[] concat(T[] first, T[] second) {
		  T[] result = Arrays.copyOf(first, first.length + second.length);
		  System.arraycopy(second, 0, result, first.length, second.length);
		  return result;
		}
	

}
