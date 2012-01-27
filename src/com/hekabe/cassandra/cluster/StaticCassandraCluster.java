package com.hekabe.cassandra.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.hekabe.cassandra.instance.CassandraInstance;
import com.hekabe.cassandra.instance.StaticCassandraInstance;
import com.hekabe.cassandra.util.YamlParameters;
import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.connection.ChannelOutputStream;
import com.sshtools.j2ssh.session.SessionChannelClient;

public class StaticCassandraCluster extends CassandraCluster {

	List<StaticCassandraInstance> instances;

	private static Logger _log = Logger.getLogger(StaticCassandraCluster.class);

	public StaticCassandraCluster(List<StaticCassandraInstance> instances) {
		this.instances = instances;
	}

	/**
	 * adapt to an already existing cluster
	 * @param ipPasswordMap 
	 * @return
	 */
	public static StaticCassandraCluster takeOverCluster(HashMap<String, String> ipPasswordMap){
		
		ArrayList<StaticCassandraInstance> instances = new ArrayList<StaticCassandraInstance>();
		Set<Map.Entry<String, String>> ips = ipPasswordMap.entrySet();
		for (Map.Entry<String, String> ipPassword : ips) {
			instances.add(new StaticCassandraInstance(ipPassword.getKey(), "root", ipPassword.getValue()));
		}
		return new StaticCassandraCluster(instances);
	}
	
	/**
	 * given servers need an ubuntu os with ssh access for user "root" 
	 * @param ipPasswordMap <ip> ==> <sshPassword>
	 * @param enableMultiRegion actually no difference between multiRegion and single region for a static cluster
	 * @return
	 */
	public static StaticCassandraCluster initializeCluster(HashMap<String, String> ipPasswordMap, boolean enableMultiRegion) {
		ArrayList<StaticCassandraInstance> instances = new ArrayList<StaticCassandraInstance>();

		Set<Map.Entry<String, String>> ips = ipPasswordMap.entrySet();
		for (Map.Entry<String, String> ipPassword : ips) {
			instances.add(new StaticCassandraInstance(ipPassword.getKey(), "root", ipPassword.getValue()));
		}

		StaticCassandraCluster cluster = new StaticCassandraCluster(instances);
		
		cluster.openFirewallPorts(ipPasswordMap.keySet());
		
		cluster.waitForClusterUpAndRunning();
		cluster.executeActionOnAllInstances("setupCassandra");
		if (enableMultiRegion) {
			cluster.configureMultiRegion();
		} else {
			cluster.configureSingleRegion();
		}
		cluster.executeActionOnAllInstances("updateCassandra");
		return cluster;
	}

	@Override
	public Collection<? extends CassandraInstance> getInstances() {
		return this.instances;
	}

	//seems that this is not needed at all
	@Override
	public void openFirewallPorts(Collection<String> ips) {

//		for (CassandraInstance ci : instances) {
//			boolean success = false;
//			do {
//
//				try {
//					SshClient sshClient = ci.connectSSH();
//
//					_log.info("authentication succeeded");
//					SessionChannelClient session = sshClient
//							.openSessionChannel();
//					ChannelOutputStream out;
//
//					session.requestPseudoTerminal("gogrid", 80, 24, 0, 0, "");
//					if (session.startShell()) {
//						out = session.getOutputStream();
//						
//						//open port 7000 in ip table for all ips
//						
//						for(String ip : ips){
//							out.write("sudo iptables -A OUTPUT -p tcp --dport 7000 -j ACCEPT\n".getBytes());
//							out.write(("iptables -A INPUT -s " + ip + " -p tcp --dport 7000 -j ACCEPT\n").getBytes());
//							_log.info("opened port 7000 on " + ci.getPublicIp());
//							TimeUnit.SECONDS.sleep(1);
//						}
//					}
//					success = true;
//				} catch (IOException e) {
//					success = false;
//					_log.error("ssh process for instance " + ci.getPublicIp()
//							+ " failed");
//					e.printStackTrace();
//					if (!success) {
//						try {
//							_log.warn("retry in 5 sec...");
//							TimeUnit.SECONDS.sleep(5);
//						} catch (InterruptedException e1) {
//							e1.printStackTrace();
//						}
//					}
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			} while (!success);
//		}
	}

	@Override
	public void configureSingleRegion() {
		configureMultiRegion();
	}

	@Override
	public void configureMultiRegion() {

		multiRegionEnabled = true;
		List<String> ips = new ArrayList<String>();
		for (CassandraInstance ci : getInstances()) {
			ci.setConfigParameter(YamlParameters.LISTEN_ADDRESS,
					ci.getPrivateIp());
			ci.setConfigParameter(YamlParameters.RPC_ADDRESS, "0.0.0.0");
			//do not set broadcast_address otherwise nodes wont find each other
//			ci.setConfigParameter(YamlParameters.BROADCAST_ADDRESS,
//					ci.getPublicIp());
			setConfigParameter(YamlParameters.ENDPOINT_SNITCH,
					"org.apache.cassandra.locator.SimpleSnitch");
			// setConfigParameter(YamlParameters.CLUSTER_NAME,
			// "multiRegionClusterTest");
			setConfigParameter(YamlParameters.SEEDS, this.getSeeds());
			ips.add(ci.getPublicIp());
		}
	}

}
