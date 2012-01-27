package com.hekabe.cassandra.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.hekabe.cassandra.instance.CassandraInstance;
import com.hekabe.cassandra.util.InitialTokens;

public abstract class CassandraCluster {

	private static Logger _log = Logger.getLogger(CassandraCluster.class);
	protected boolean multiRegionEnabled = false;

	public void rebalanceTokens(){
		int numerOfNodes = getInstances().size();
		String[] tokens = InitialTokens.getTokens(numerOfNodes);
		int i=0;
		for(CassandraInstance ci : getInstances()){
			try {
				ci.moveNode(tokens[i]);
			} catch (IOException e) {
				_log.error("moving node token for " + ci.getPublicIp() + " failed");
				e.printStackTrace();
			}
			i++;
		}
	}
	
	public void setConfigParameter(String configParameter, String value) {
		for (CassandraInstance instance : getInstances()) {
			instance.setConfigParameter(configParameter, value);
		}
	}	
	
	public void executeActionOnAllInstances(String action){
		waitForClusterUpAndRunning();
		CyclicBarrier cb = new CyclicBarrier(getInstances().size() + 1);
		try {
		for (CassandraInstance ci : getInstances()) {
			_log.info("## running " + action + " on " + ci.getPublicIp());
			ci.executeAction(action, cb);
			//avoid conflicts with parallel startups
			TimeUnit.SECONDS.sleep(5);
		}
			_log.info("## waiting for all instances");
			cb.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}
	}	
	protected void waitForClusterUpAndRunning() {
		boolean running = false;
		while (!running) {
			running = true;
			for (CassandraInstance instance : getInstances()) {
				if (!instance.isInstanceRunning()) {
					running = false;
				}
			}
			if (!running) {
				_log.info("instance is not up yet, waiting another 10 sec");
				try {
					TimeUnit.SECONDS.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}	
	
	public String getSeeds() {
		String seeds = "";
		for (CassandraInstance instance : getInstances()) {
			if (isMultiRegionEnabled()) {
				seeds += instance.getPublicIp() + ",";
			} else {
				seeds += instance.getPrivateIp() + ",";
			}

		}
		seeds = seeds.substring(0, seeds.length() - 1);
		return seeds;
	}
	
	public List<String> getPublicIps() {
		List<String> publicIps = new ArrayList<String>();

		for (CassandraInstance instance : getInstances()) {
			publicIps.add(instance.getPublicIp());
		}
		return publicIps;
	}

	public abstract Collection<? extends CassandraInstance> getInstances();
	
	public abstract void openFirewallPorts(Collection<String> list);
	
	public boolean isMultiRegionEnabled() {
		return multiRegionEnabled;
	}	
	public abstract void configureSingleRegion();
	
	public abstract void configureMultiRegion();

}
