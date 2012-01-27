package com.hekabe.cassandra.instance;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.SshException;
import com.sshtools.j2ssh.authentication.AuthenticationProtocolState;
import com.sshtools.j2ssh.authentication.PublicKeyAuthenticationClient;
import com.sshtools.j2ssh.transport.IgnoreHostKeyVerification;
import com.sshtools.j2ssh.transport.publickey.SshPrivateKey;
import com.sshtools.j2ssh.transport.publickey.SshPrivateKeyFile;

/**
 * cassandra node on ec2 instance
 * @author Holger Trittenbach
 *
 */
public class EC2CassandraInstance extends CassandraInstance {

	private static Logger _log = Logger.getLogger(EC2CassandraInstance.class);
	
	private String instanceId;
	private AmazonEC2 ec2;
	private File keyFile;
	
	/**
	 * 
	 * @param instanceId ec2 instance id
	 * @param ec2 amazon credentials connection
	 * @param keyFile privateKey for ssh connection
	 */
	public EC2CassandraInstance(String instanceId, AmazonEC2 ec2, File keyFile) {
		this.instanceId = instanceId;
		this.ec2 = ec2;
		this.keyFile = keyFile;
		cassandraParameters = new HashMap<String, String>();
	}

	
	/**
	 * 
	 * @return representation of the ec2Instance the cassandra node is running at 
	 */
	public Instance getAWSInstanceReference() {
		DescribeInstancesRequest instanceRequest = new DescribeInstancesRequest();
		instanceRequest.getInstanceIds().add(instanceId);
		DescribeInstancesResult instanceResult = ec2
				.describeInstances(instanceRequest);
		Instance instance = instanceResult.getReservations().get(0)
				.getInstances().get(0);
		return instance;
	}
		
	/**
	 * 
	 * @return ec2 credentials Instance
	 */
	public AmazonEC2 getEc2() {
		return ec2;
	}
	
	/**
	 * 
	 * @return amazon instance id
	 */
	public String getInstanceId() {
		return instanceId;
	}
	
	/**
	 * set the conneciton credentials for ec2
	 * @param ec2
	 */
	public void setEc2(AmazonEC2 ec2) {
		this.ec2 = ec2;
	}

	/*
	 * (non-Javadoc)
	 * @see com.hekabe.aws.CassandraInstance#isInstanceRunning()
	 */
	public boolean isInstanceRunning(){
		boolean result = false;
		String ip = this.getAWSInstanceReference().getPublicIpAddress();
		if(ip!=null){
			result = true;
		}
		return result;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.hekabe.aws.CassandraInstance#getPublicIp()
	 */
	public String getPublicIp(){
		return this.getAWSInstanceReference().getPublicIpAddress();
	}
	
	/**
	 * 
	 * @return the public dns name for the ec2 instance
	 */
	public String getDnsName(){
		return this.getAWSInstanceReference().getPublicDnsName();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.hekabe.aws.CassandraInstance#getPrivateIp()
	 */
	public String getPrivateIp(){
		return this.getAWSInstanceReference().getPrivateIpAddress();
	}
	
	public SshClient connectSSH() throws IOException {
		PublicKeyAuthenticationClient authClient = new PublicKeyAuthenticationClient();

		// connect to the instance
		SshPrivateKeyFile sshPrivKeyFile = SshPrivateKeyFile.parse(keyFile);
		SshPrivateKey sshPrivKey = sshPrivKeyFile.toPrivateKey("");
		authClient.setKey(sshPrivKey);
		authClient.setUsername("ubuntu");

		SshClient sshClient = new SshClient();
		sshClient.connect(this.getPublicIp(), new IgnoreHostKeyVerification());
		int authResult = sshClient.authenticate(authClient);
		if (authResult != AuthenticationProtocolState.COMPLETE) {
			throw new SshException(
					"AuthenticationProtocolState is not COMPLETE");
		}
		_log.info("authentication complete");
		return sshClient;
	}
}
