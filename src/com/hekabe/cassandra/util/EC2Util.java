package com.hekabe.cassandra.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.UserIdGroupPair;

public class EC2Util {

	private static Logger _log = Logger.getLogger(EC2Util.class);

	public static void terminateInstance(AmazonEC2 ec2, List<String> instanceIds) {

		 TerminateInstancesRequest terminateRequest = new
		 TerminateInstancesRequest().withInstanceIds(instanceIds);
		 ec2.terminateInstances(terminateRequest);
	}
			
	public static File createKeyPair(String keyName, AmazonEC2 ec2) {
		File keyFile = null;
		// create new RSA key
		CreateKeyPairRequest keyPairRequest = new CreateKeyPairRequest(keyName);
		CreateKeyPairResult keyPairResult = ec2.createKeyPair(keyPairRequest);

		try {
			//keyFile = new File("C:\\Users\\Holger\\Desktop" +keyName + ".pem");
			keyFile = new File(keyName+ ".pem");
			keyFile.createNewFile();
			
			// keyFile.deleteOnExit();

			BufferedWriter out = new BufferedWriter(new FileWriter(keyFile));
			// System.out.println(keyPairResult.getKeyPair().getKeyMaterial());
			out.write(keyPairResult.getKeyPair().getKeyMaterial());
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return keyFile;
	}
	
	public static String creatSecurityGroup(AmazonEC2 ec2, String groupName){
		CreateSecurityGroupRequest sgRequest = new CreateSecurityGroupRequest().withGroupName(groupName).withDescription("cassandra cluster security group");
		CreateSecurityGroupResult sgResult = ec2.createSecurityGroup(sgRequest);
		String groupId = sgResult.getGroupId();
		List<IpPermission> permissions = new ArrayList<IpPermission>();
		permissions.add(new IpPermission().withFromPort(22).withToPort(22).withIpRanges("0.0.0.0/0").withIpProtocol("tcp"));
		permissions.add(new IpPermission().withFromPort(7199).withToPort(7199).withIpRanges("0.0.0.0/0").withIpProtocol("tcp"));
		permissions.add(new IpPermission().withFromPort(9160).withToPort(9160).withIpRanges("0.0.0.0/0").withIpProtocol("tcp"));
		//opscenter is not used
		//permissions.add(new IpPermission().withFromPort(1024).withToPort(65535).withUserIdGroupPairs(new UserIdGroupPair().withGroupId(groupId)).withIpProtocol("tcp"));
		permissions.add(new IpPermission().withFromPort(7000).withToPort(7000).withUserIdGroupPairs(new UserIdGroupPair().withGroupId(groupId)).withIpProtocol("tcp"));

		AuthorizeSecurityGroupIngressRequest rule = new AuthorizeSecurityGroupIngressRequest().withIpPermissions(permissions).withGroupId(sgResult.getGroupId());
		ec2.authorizeSecurityGroupIngress(rule);
		_log.info("added security group with id: " + groupId);
		return groupId;
	}
	
	public static void addSecurityGroupRule(String securityGroupId, AmazonEC2 ec2, Integer fromPort, Integer toPort, String ip) {

		List<IpPermission> permissions = new ArrayList<IpPermission>();
		permissions.add(new IpPermission().withFromPort(fromPort)
				.withToPort(toPort).withIpRanges(ip + "/32")
				.withIpProtocol("tcp"));
		AuthorizeSecurityGroupIngressRequest rule = new AuthorizeSecurityGroupIngressRequest()
				.withIpPermissions(permissions).withGroupId(securityGroupId);
		ec2.authorizeSecurityGroupIngress(rule);

		}
}
