package com.hekabe.cassandra.util;

import java.math.*;

/**
 * @author Holger Trittenbach
 *
 */
public class InitialTokens {

	/**
	 * method to claculate the right tokens for a balanced ring. reference: Edward Capriolo : Cassandra High Performance Cookbook
	 * @param numberOfNodes
	 * @return
	 */
	public static String[] getTokens(int numberOfNodes){
		String[] tokens = new String[numberOfNodes];
		for(int i = 0; i<numberOfNodes; i++){
			BigInteger hs = new BigInteger("2");
			BigInteger res = hs.pow(127);
			BigInteger div = res.divide(new BigInteger(numberOfNodes + ""));
			BigInteger fin = div.multiply(new BigInteger(i+""));
			tokens[i] = fin.toString();	
		}
		return tokens;
	}
}
