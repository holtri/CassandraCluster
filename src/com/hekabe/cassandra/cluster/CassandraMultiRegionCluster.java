package com.hekabe.cassandra.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.hekabe.cassandra.instance.CassandraInstance;
import com.hekabe.cassandra.util.InitialTokens;
import com.hekabe.cassandra.util.YamlParameters;

public class CassandraMultiRegionCluster {

	private static Logger _log = Logger.getLogger(CassandraMultiRegionCluster.class);
	
	private List<CassandraCluster> clusters;
	private String seeds;
	private List<String> publicIps;

	public void rebalanceTokens(){
		List<CassandraInstance> instances = new ArrayList<CassandraInstance>();
		for(CassandraCluster cc : clusters){
			instances.addAll(cc.getInstances());
		}
		String[] tokens = InitialTokens.getTokens(instances.size());
		int i=0;
		for(CassandraInstance ci : instances){
			try {
				ci.moveNode(tokens[i]);
			} catch (IOException e) {
				_log.error("moving node token for " + ci.getPublicIp() + " failed");
				e.printStackTrace();
			}
			i++;
		}
	}
	
	public void addCluster(CassandraCluster cluster) {
		// ensure all clusters are available
		this.waitForMultiRegionClusterUpAndRunning();
		cluster.waitForClusterUpAndRunning();
		// open ports in the old clusterpart
		for (CassandraCluster c : clusters) {
			c.openFirewallPorts(cluster.getPublicIps());
		}
		//open ports in the new clusterpart 
		cluster.openFirewallPorts(publicIps);
		// add public ips to multiregion cluster
		this.publicIps.addAll(cluster.getPublicIps());

		//ensure that cluster is multiregion enabled
		if(!cluster.isMultiRegionEnabled()){
			cluster.configureMultiRegion();
		}
		
		// setup parameters for new cluster instances
		for (CassandraInstance ci : cluster.getInstances()) {
			ci.setConfigParameter(YamlParameters.AUTO_BOOTSTRAP, "true");
			ci.setConfigParameter(YamlParameters.SEEDS, seeds);
			ci.setConfigParameter(YamlParameters.INITIAL_TOKEN, "");
			ci.executeAction("updateCassandra");
			ci.executeAction("restartCassandra");
			try {
				_log.info("## adding instance " + ci.getPublicIp() + " to the cluster...");
				TimeUnit.SECONDS.sleep(30);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		

		try {
			TimeUnit.SECONDS.sleep(60);
			_log.info("sleep 180 sec");

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		this.seeds = seeds + "," + cluster.getSeeds();
		this.clusters.add(cluster);
		
		//update all to new config
		//no restart needed now, can be done later on
		for(CassandraCluster c : clusters){
			c.setConfigParameter(YamlParameters.SEEDS, this.seeds);
			c.setConfigParameter(YamlParameters.AUTO_BOOTSTRAP, "false");
			c.executeActionOnAllInstances("updateCassandra");
		}
		_log.info("## finished with merging clusters");
	}

	private CassandraMultiRegionCluster(CassandraCluster cluster) {
		cluster.waitForClusterUpAndRunning();
		
		this.clusters = new ArrayList<CassandraCluster>();
		this.publicIps = new ArrayList<String>();
		
		this.clusters.add(cluster);
		this.seeds = cluster.getSeeds();
		this.publicIps.addAll(cluster.getPublicIps());
		
	}

	public void waitForMultiRegionClusterUpAndRunning() {
		for (CassandraCluster c : clusters) {
			c.waitForClusterUpAndRunning();
		}
	}
	
	public static CassandraMultiRegionCluster createMultiRegionCluster(CassandraCluster cluster){
		if(!cluster.isMultiRegionEnabled()){
			_log.warn("multi region has not been enabled! enabling multi region for the cluster NOW ...");
			cluster.executeActionOnAllInstances("stopCassandra");
			cluster.configureMultiRegion();
			cluster.executeActionOnAllInstances("startCassandra");
		}
		return new CassandraMultiRegionCluster(cluster);		
	}
	
	public static CassandraMultiRegionCluster takeOverMultiRegionCluster(List<CassandraCluster> clusters){
		
		CassandraMultiRegionCluster mrC = new CassandraMultiRegionCluster(clusters.get(0));
		clusters.remove(0);
		for(CassandraCluster c : clusters){
			mrC.clusters.add(c);
			mrC.seeds = mrC.seeds + "," + c.getSeeds();
			mrC.publicIps.addAll(c.getPublicIps());
		}
		return mrC;
	}
}
