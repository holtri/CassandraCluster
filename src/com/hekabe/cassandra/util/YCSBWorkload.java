package com.hekabe.cassandra.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.hekabe.cassandra.cluster.AbstractCassandraCluster;
import com.hekabe.cassandra.cluster.CassandraCluster;
import com.hekabe.cassandra.instance.StaticCassandraInstance;
import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.SshException;
import com.sshtools.j2ssh.authentication.AuthenticationProtocolState;
import com.sshtools.j2ssh.authentication.PasswordAuthenticationClient;
import com.sshtools.j2ssh.connection.ChannelOutputStream;
import com.sshtools.j2ssh.session.SessionChannelClient;
import com.sshtools.j2ssh.transport.IgnoreHostKeyVerification;

public class YCSBWorkload {

	private static Logger _log = Logger.getLogger(YCSBWorkload.class);

	private String username;
	private String password;
	private String ip;
	
	//workload parameters
	final static String RECORD_COUNT = "recordcount";
	final static String OPERATION_COUNT = "operationcount";
	final static String WORKLOAD = "workload";
	final static String READ_ALL_FIELDS = "readallfields";
	final static String READ_PROPORTION = "readproportion";
	final static String UPDATE_PROPORTION = "updateproportion";
	final static String SCAN_PROPORTION = "scanproportion";
	final static String INSERT_PROPORTION = "insertproportion";
	final static String REQUEST_DISTRIBUTION = "requestdistribution";
	final static String INSERT_START = "insertstart";
	final static String INSERT_COUNT ="insertcount";
	
	private HashMap<String, String> parameters = new HashMap<String, String>();
		
	
	public YCSBWorkload(String ip, String username, String password){
		this.ip = ip;
		this.username = username;
		this.password = password;
		
		//default data
		parameters.put(RECORD_COUNT, "1000");
		parameters.put(OPERATION_COUNT, "1000");
		parameters.put(WORKLOAD, "com.yahoo.ycsb.workloads.CoreWorkload");
		parameters.put(READ_ALL_FIELDS, "true");
		parameters.put(READ_PROPORTION, "0");
		parameters.put(UPDATE_PROPORTION, "0");
		parameters.put(SCAN_PROPORTION, "0.5");
		parameters.put(UPDATE_PROPORTION, "0");
		parameters.put(INSERT_PROPORTION, "1");
		parameters.put(REQUEST_DISTRIBUTION, "zipfian");
		parameters.put(INSERT_START, "0");
		parameters.put(INSERT_COUNT, "1000");

	}
	
	public void setParameter(String key, String value){
		parameters.put(key, value);
	}
	
	public String getParameter(String key){
		return parameters.get(key);
	}
	/**
	 * 
	 * @return String for ycsb call
	 */
	String buildParameterString(){
		StringBuffer parameterlist = new StringBuffer();
//		parameterlist.append("\"");
		for(Map.Entry<String, String> entry : parameters.entrySet()){
			parameterlist.append("-p");
			parameterlist.append(" " + entry.getKey());
			parameterlist.append("=" + entry.getValue());
			parameterlist.append(" ");
		}
//		parameterlist.append("\"");
		return parameterlist.toString();
	}
	/**
	 * 
	 * @param cluster cluster to be benchmarked
	 * @param load true, if this run is to load data into the cluster
	 * @param outputFileName filename the output will be stored in (without postfix, means no ".csv", just the name)
	 * @param workloadFile workload file that is used for benchmark (expamle: workloads/workloadb). If null, default data workloads/workloada will be used. 
	 * @throws IOException
	 */
	public void runBenchmark(AbstractCassandraCluster cluster, boolean load, String outputFileName, String workloadFile) throws IOException{
		
		//connect ssh
		
		PasswordAuthenticationClient authClient = new PasswordAuthenticationClient();
		authClient.setUsername(username);
		authClient.setPassword(password);
		
		SshClient sshClient = new SshClient();
		sshClient.connect(ip, new IgnoreHostKeyVerification());
		
		int authResult = sshClient.authenticate(authClient);
		if (authResult != AuthenticationProtocolState.COMPLETE) {
			throw new SshException(
					"AuthenticationProtocolState is not COMPLETE");
		}
		_log.info("authentication complete");
		
		SessionChannelClient session = sshClient.openSessionChannel();
		ChannelOutputStream out = null;

		session.requestPseudoTerminal("gogrid", 80, 24, 0, 0, "");
		if (session.startShell()) {
			out = session.getOutputStream();
			
			//runycsb.sh hosts workloadfile parameters downloadfilename
			
			String hosts = "";
			
			for(String s : cluster.getPublicIps()){
				hosts = hosts + s + ",";
			}
			hosts = hosts.substring(0, hosts.length()-1);
			
			if(workloadFile==null){
				workloadFile = "workloads/workloada";
			}
			String parameters = buildParameterString();
			
			if(load){
				parameters = "\"-load " +parameters + " \"";
			}else{
				parameters = "\"-t " + parameters + "\"";
			}
			
			_log.info("./runycsb.sh " + hosts + " " + workloadFile + " " + parameters + " " + outputFileName + "\n");
			out.write("cd YCSB\n".getBytes());
			out.write(("./runycsb.sh " + hosts + " " + workloadFile + " " + parameters + " " + outputFileName +"\n").getBytes());
			try {
				TimeUnit.SECONDS.sleep(3);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			out.close();
		}
	}	
}
