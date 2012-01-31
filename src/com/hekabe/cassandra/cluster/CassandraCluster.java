package com.hekabe.cassandra.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.hekabe.cassandra.instance.CassandraInstance;
import com.hekabe.cassandra.util.InitialTokens;
import com.hekabe.cassandra.util.YCSBWorkload;
import com.hekabe.cassandra.util.YamlParameters;

public abstract class CassandraCluster extends AbstractCassandraCluster{

	private static Logger _log = Logger.getLogger(CassandraCluster.class);
	protected boolean multiRegionEnabled = false;

	public void rebalanceTokens() {
		int numerOfNodes = getInstances().size();
		String[] tokens = InitialTokens.getTokens(numerOfNodes);
		int i = 0;
		for (CassandraInstance ci : getInstances()) {
			try {
				ci.moveNode(tokens[i]);
			} catch (IOException e) {
				_log.error("moving node token for " + ci.getPublicIp()
						+ " failed");
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

	public void executeActionOnAllInstances(String action) {
		waitForClusterUpAndRunning();
		CyclicBarrier cb = new CyclicBarrier(getInstances().size() + 1);
		try {
			for (CassandraInstance ci : getInstances()) {
				_log.info("## running " + action + " on " + ci.getPublicIp());
				ci.executeAction(action, cb);
				// avoid conflicts with parallel startups
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

	//public abstract Collection<? extends CassandraInstance> getInstances();

	public abstract void openFirewallPorts(Collection<String> list);

	public boolean isMultiRegionEnabled() {
		return multiRegionEnabled;
	}

	public abstract void configureSingleRegion();

	public abstract void configureMultiRegion();

}
