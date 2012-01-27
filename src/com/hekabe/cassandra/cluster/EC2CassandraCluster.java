package com.hekabe.cassandra.cluster;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.hekabe.cassandra.instance.CassandraInstance;
import com.hekabe.cassandra.instance.EC2CassandraInstance;
import com.hekabe.cassandra.util.EC2Util;
import com.hekabe.cassandra.util.YamlParameters;

public class EC2CassandraCluster extends CassandraCluster {

	private static Logger _log = Logger.getLogger(EC2CassandraCluster.class);

	protected List<EC2CassandraInstance> instances;
	private String securityGroupId;

	public EC2CassandraCluster(List<EC2CassandraInstance> instances, String securityGroupId) {
		this.instances = instances;
		this.securityGroupId = securityGroupId;
	}

	public static EC2CassandraCluster takeOverCassandraCluster(AmazonEC2 ec2, HashMap<String, String> tagKeyValue, File keyFile){
		DescribeInstancesRequest describeInstance = new DescribeInstancesRequest();
		for(Map.Entry<String, String> keyValue : tagKeyValue.entrySet()){
			_log.info("adding search tag with key: " + keyValue.getKey() + " and value " + keyValue.getValue());
			describeInstance.withFilters(new Filter("tag:" + keyValue.getKey()).withValues(keyValue.getValue()));
		}
		
		DescribeInstancesResult describeInstanceResult = ec2.describeInstances(describeInstance);
		List<Reservation> reservations = describeInstanceResult
				.getReservations();
		
		ArrayList<EC2CassandraInstance> cassandraInstances = new ArrayList<EC2CassandraInstance>();
		
		String securityGroupId = null;
		for (Reservation r : reservations) {
			List<Instance> instances = r.getInstances();
			for (Instance i : instances) {
				cassandraInstances.add(new EC2CassandraInstance(i.getInstanceId(), ec2, keyFile));
				_log.info("found instance: " + i.getPublicIpAddress());
				i.getSecurityGroups().get(0).getGroupId();
			}
		}
		_log.info("securityGroupId for cluster is : " + securityGroupId);
		return new EC2CassandraCluster(cassandraInstances, securityGroupId);
	}
	
	/**
	 * WARNING: clusterName actually only used for tagging
	 * @param ec2
	 * @param imageId
	 * @param instanceType
	 * @param clusterSize
	 * @param securityGroupName
	 * @param keyName
	 * @param clusterName
	 * @param enableMultiRegion
	 * @return
	 */
	public static EC2CassandraCluster initializeCluster(AmazonEC2 ec2,
			String imageId, String instanceType, int clusterSize,
			String securityGroupName, String keyName, String clusterName, boolean enableMultiRegion) {
		// generate KeyFile
		File keyFile = EC2Util.createKeyPair(keyName, ec2);

		// create SecurityGroup
		String securityGroupId = EC2Util.creatSecurityGroup(ec2,
				securityGroupName);
		_log.info("security group created with id: " + securityGroupId);
		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		ArrayList<EC2CassandraInstance> cassandraInstances = new ArrayList<EC2CassandraInstance>();

		// create instances
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
				.withInstanceType(instanceType).withImageId(imageId)
				.withMinCount(clusterSize).withMaxCount(clusterSize)
				.withSecurityGroupIds(securityGroupId).withKeyName(keyName);

		RunInstancesResult runInstances = ec2.runInstances(runInstancesRequest);

		// tag ec2 instances
		List<Instance> instances = runInstances.getReservation().getInstances();
		_log.warn("## number of launched instances: " + instances.size());
		for (Instance instance : instances) {
			CreateTagsRequest createTagsRequest = new CreateTagsRequest();
			createTagsRequest.withResources(instance.getInstanceId()).withTags(
					new Tag("cassandraCluster", clusterName));
			ec2.createTags(createTagsRequest);
			_log.info("Instance with id " + instance.getInstanceId()
					+ " startet");
			cassandraInstances.add(new EC2CassandraInstance(instance
					.getInstanceId(), ec2, keyFile));
		}

		EC2CassandraCluster cluster = new EC2CassandraCluster(
				cassandraInstances, securityGroupId);
		_log.info("clustersize: " + cluster.getInstances().size());
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
	public void openFirewallPorts(Collection<String> ips) {
		if (!instances.isEmpty()) {
			for (String ip : ips) {
				EC2Util.addSecurityGroupRule(this.securityGroupId,
						this.instances.get(0).getEc2(), 7000, 7000, ip);
			}
			_log.info("opened firewall ports");
		}
	}

	public List<String> getClusterDnsNames() {
		ArrayList<String> dnsNames = new ArrayList<String>();
		for (EC2CassandraInstance instance : instances) {
			dnsNames.add(instance.getAWSInstanceReference().getPublicDnsName());
		}
		return dnsNames;
	}

	public List<EC2CassandraInstance> getInstances() {
		return instances;
	}

	public void configureMultiRegion() {
		multiRegionEnabled = true;
		List<String> ips = new ArrayList<String>();
		for (CassandraInstance ci : getInstances()) {
			ci.setConfigParameter(YamlParameters.LISTEN_ADDRESS, ci.getPrivateIp());
			ci.setConfigParameter(YamlParameters.RPC_ADDRESS, "0.0.0.0");
			ci.setConfigParameter(YamlParameters.BROADCAST_ADDRESS,
					ci.getPublicIp());
			setConfigParameter(YamlParameters.ENDPOINT_SNITCH,
					"org.apache.cassandra.locator.Ec2MultiRegionSnitch");
			//setConfigParameter(YamlParameters.CLUSTER_NAME, "multiRegionClusterTest");
			setConfigParameter(YamlParameters.SEEDS, this.getSeeds());
			ips.add(ci.getPublicIp());
		}
		openFirewallPorts(ips);
	}
	public void configureSingleRegion() {
		multiRegionEnabled = false;
		for (CassandraInstance ci : getInstances()) {
			ci.setConfigParameter(YamlParameters.LISTEN_ADDRESS,
					ci.getPrivateIp());
			ci.setConfigParameter(YamlParameters.RPC_ADDRESS, "0.0.0.0");
			setConfigParameter(YamlParameters.ENDPOINT_SNITCH,
					"org.apache.cassandra.locator.Ec2Snitch");
			//setConfigParameter(YamlParameters.CLUSTER_NAME, "singleRegionClusterTest");
			setConfigParameter(YamlParameters.SEEDS, this.getSeeds());
		}
	}
}
