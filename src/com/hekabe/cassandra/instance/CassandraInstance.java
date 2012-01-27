package com.hekabe.cassandra.instance;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.connection.ChannelOutputStream;
import com.sshtools.j2ssh.session.SessionChannelClient;

public abstract class CassandraInstance {
	
	private static Logger _log = Logger.getLogger(CassandraInstance.class);
	private boolean cassandraInstalled = false;
	protected  HashMap<String, String> cassandraParameters = new HashMap<String, String>();

	/**
	 * starts a new update Thread that uses the action string to execute the wanted method asynchronous
	 * @param keyFile ssh private key
	 * @param action the action that is executed
	 * @return startet Thread
	 */
	public Thread executeAction(String action){
		Thread updater = null;
		if(isInstanceRunning()){
			updater = new InstanceUpdater(this, action, new CyclicBarrier(1));
			updater.start();
		}
		else{
			_log.error("instance is not up yet. unable to run device");
		}
		return updater;
	}
	
	/**
	 * starts a new update Thread that uses the action string to execute the wanted method asynchronous
	 * @param keyFile ssh private key
	 * @param action the action that is executed
	 * @param cb cyclic barrier to await after Thread has finished
	 * @return startet Thread
	 */
	public Thread executeAction(String action, CyclicBarrier cb){
		Thread updater = null;
		if(isInstanceRunning()){
			updater = new InstanceUpdater(this, action, cb);
			updater.start();
		}
		else{
			_log.error("instance is not up yet. unable to run device");
		}
		return updater;
	}
	
	public boolean isCassandraInstalled() {
		return cassandraInstalled;
	}

	public void setCassandraInstalled(boolean cassandraInstalled) {
		this.cassandraInstalled = cassandraInstalled;
	}
	
	public void setConfigParameter(String configParameter, String value){
		cassandraParameters.put(configParameter, value);
	}
	
	public HashMap<String, String> getCassandraParameters() {
		return cassandraParameters;
	}
	/**
	 * check the connection to the instance if it is reachable or not
	 * @return
	 */
	public abstract boolean isInstanceRunning();
	
	/**
	 * 
	 * @return the public ip adress you can use to connect to the instance
	 */
	public abstract String getPublicIp();

	/**
	 * ip for an internal cluster subnet
	 * @return
	 */
	public abstract String getPrivateIp();
	
	public void waitNodeUpAndRunning(){
		while(!isInstanceRunning()){
			_log.warn("instance " + getPublicIp() + " is not up yet. waiting 10 sec...");
			try {
				TimeUnit.SECONDS.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public boolean moveNode(String initialToken) throws IOException{
		boolean result = false;
		SshClient sshClient = connectSSH();
		_log.info("authentication succeeded");
		SessionChannelClient session = sshClient.openSessionChannel();
		ChannelOutputStream out;

		session.requestPseudoTerminal("gogrid", 80, 24, 0, 0, "");
		if (session.startShell()) {
			try {
				out = session.getOutputStream();
				out.write(("sudo nodetool -h localhost move " + initialToken + "\n").getBytes());
				_log.info("node " + this.getPublicIp() + " is starting to be moved to initial token " + initialToken + ". this may take a lot of time to be finished");
				TimeUnit.SECONDS.sleep(2);
				result = true;
				
			} catch (InterruptedException e) {
				e.printStackTrace();
				result = false;
			}
		}
		return result;
	}

	public abstract SshClient connectSSH() throws IOException;
}
