package com.hekabe.cassandra.instance;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.connection.ChannelOutputStream;
import com.sshtools.j2ssh.session.SessionChannelClient;
import com.sshtools.j2ssh.session.SessionOutputReader;
import com.sshtools.j2ssh.transport.publickey.InvalidSshKeyException;

public class InstanceUpdater extends Thread{
	
	private static Logger _log = Logger.getLogger(InstanceUpdater.class);

	protected CassandraInstance instance;
	protected String action;
	protected CyclicBarrier cb;

	/**
	 * 
	 * @param instance cassandra node on ec2 infrastructure that has to be updated
	 * @param keyFile private key for ssh access
	 * @param action methodname for the method the thread should execute in its run method if you run the update asynchronous
	 * setupCassandra: installs cassandra with a setup script
	 * updateCassandra: run the update script for cassandra. the update script will take all configured parameters of the instance into consideration (cassandraParameters)
	 * startCassandra: start the cassandra process
	 * stopCassandra: stop the cassandra process
	 * restartCassandra: restart the cassandra process
	 * @param cb thread join element to wait for a bunch of instances to complete serverside configuration
	 */
	public InstanceUpdater(CassandraInstance instance, String action, CyclicBarrier cb) {
		this.instance = instance;
		this.action = action;
		this.cb = cb;
	}

	public void run() {
		boolean success = false;
		do {
			try {

				if (action == "setupCassandra") {
					success = this.setupCassandra();
					if (!success) {
						_log.error("setupCassandra was not executed correctly");
					}
				}
				if (action == "updateCassandra") {
					success = this.runUpdateScript();
				}
				if (action == "startCassandra") {
					success = this.cassandraDevice("start");
				}
				if (action == "stopCassandra") {
					success = this.cassandraDevice("stop");
				}
				if (action == "restartCassandra") {
					success = this.cassandraDevice("restart");
				}
				if (action == "decommissionNode"){
					success = this.decommission();
				}

			} catch (IOException e) {
				success = false;
				_log.error("ssh process for instance "
						+ instance.getPublicIp() + " failed");
				e.printStackTrace();
				if (!success) {
					try {
						_log.warn("retry in 5 sec...");
						TimeUnit.SECONDS.sleep(5);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
		} while (!success);
		_log.info(this.instance.getPublicIp()
				+ " is waiting for other instances to complete the operation");
		try {
			cb.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}

	}

	private boolean decommission() throws IOException {
		boolean result = false;
		SshClient sshClient = connectSSH();
		_log.info("authentication succeeded");
		SessionChannelClient session = sshClient.openSessionChannel();
		ChannelOutputStream out;

		session.requestPseudoTerminal("gogrid", 80, 24, 0, 0, "");
		if (session.startShell()) {
			try {
				out = session.getOutputStream();
				out.write("sudo nodetool -h localhost decommission\n".getBytes());
				_log.info("node " + instance.getPublicIp() + " is starting to be removed from the cluster. this may take a lot of time to be finished");
				TimeUnit.SECONDS.sleep(2);
				result = true;
				
			} catch (InterruptedException e) {
				e.printStackTrace();
				result = false;
			}
		}
		return result;
	}

	public SshClient connectSSH() throws IOException{
		return this.instance.connectSSH();
	}

	/**
	 * installs Cassandra on the instance with running the hekabe_install.sh script
	 * @return true, if the script was successfully installed. false, if cassandra is already installed or any failure occured
	 * @throws IOException
	 */
	public boolean setupCassandra() throws IOException {
		boolean result = false;
		if (instance.isCassandraInstalled()) {
			_log.warn("cassandra already installed");
			return false;
		}

		_log.info("starting ssh to set up cassandra");
//		PublicKeyAuthenticationClient authClient = new PublicKeyAuthenticationClient();
//
//		// connect to the instance
//		SshPrivateKeyFile sshPrivKeyFile = SshPrivateKeyFile.parse(keyFile);
//		SshPrivateKey sshPrivKey = sshPrivKeyFile.toPrivateKey("");
//		authClient.setKey(sshPrivKey);
//		authClient.setUsername("ubuntu");

		SshClient sshClient = connectSSH();
//		sshClient.connect(instance.getPublicIp(),
//				new IgnoreHostKeyVerification());
//		int authResult = sshClient.authenticate(authClient);

		// if connected and authenticated, start ssh communication
//		if (authResult == AuthenticationProtocolState.COMPLETE) {
			_log.info("authentication succeeded");
			SessionChannelClient session = sshClient.openSessionChannel();
			ChannelOutputStream out;

			session.requestPseudoTerminal("gogrid", 80, 24, 0, 0, "");
			if (session.startShell()) {
				try {
					out = session.getOutputStream();
					_log.info("getting install and configuration script");

					out.write("sudo wget http://dl.dropbox.com/u/3027291/hekabe_install.sh\n"
							.getBytes());
					out.write("sudo chmod a+x hekabe_install.sh\n".getBytes());
//					out.write("sudo wget http://dl.dropbox.com/u/3027291/yamlConfig.py\n"
//							.getBytes());
//					out.write("sudo chmod a+x yamlConfig.py\n".getBytes());


					TimeUnit.SECONDS.sleep(3);

					out.write("sudo ./hekabe_install.sh\n".getBytes());
					_log.info("installing cassandra...");

					TimeUnit.SECONDS.sleep(60);
					this.instance.setCassandraInstalled(false);
					_log.info("cassandra successfully installed on instance "
							+ instance.getPublicIp());
					result = true;
					
				} catch (InterruptedException e) {
					e.printStackTrace();
					result = false;
				}
			}
		return result;
	}

	/**
	 * runs the update script that changed the cassandra.yaml parameters to the cassandraParamters configured for the instance  
	 * @return
	 * @throws InvalidSshKeyException
	 * @throws IOException
	 */
	public boolean runUpdateScript() throws InvalidSshKeyException, IOException {

		boolean result = false;

//		PublicKeyAuthenticationClient authClient = new PublicKeyAuthenticationClient();
//
//		// connect to the instance
//		SshPrivateKeyFile sshPrivKeyFile = SshPrivateKeyFile.parse(keyFile);
//		SshPrivateKey sshPrivKey = sshPrivKeyFile.toPrivateKey("");
//		authClient.setKey(sshPrivKey);
//		authClient.setUsername("ubuntu");
//
		SshClient sshClient = connectSSH();
//		SshClient sshClient = new SshClient();
//		sshClient.connect(instance.getPublicIp(),
//				new IgnoreHostKeyVerification());
//		int authResult = sshClient.authenticate(authClient);
//		if (authResult == AuthenticationProtocolState.COMPLETE) {

			SessionChannelClient session = sshClient.openSessionChannel();
			SessionOutputReader sor = new SessionOutputReader(session);
			ChannelOutputStream out;
			int outputPos = 0;

			session.requestPseudoTerminal("gogrid", 80, 24, 0, 0, "");
			if (session.startShell()) {
				out = session.getOutputStream();
				String answer = null;
				String aux = null;
				// go through all parameters set for the instance
				_log.info("set up parameters for instance");
				Set<Map.Entry<String, String>> parameterSet = instance
						.getCassandraParameters().entrySet();
				for (Map.Entry<String, String> parameter : parameterSet) {
					out.write(("sudo python yamlConfig.py "
							+ parameter.getKey() + " " + parameter.getValue() + "\n")
							.getBytes());
					try {
						TimeUnit.SECONDS.sleep(1);
						_log.info("set " + parameter.getKey() + " to "
								+ parameter.getValue() + ". sleep 1 sec...");
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				// cut console output
				aux = sor.getOutput();
				answer = aux.substring(outputPos);
				outputPos = aux.length();
				// show script log output
				out.write("tail updateConfig.log\n".getBytes());
				try {
					TimeUnit.SECONDS.sleep(2);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				aux = sor.getOutput();
				answer = aux.substring(outputPos);
				outputPos = aux.length();
				_log.info(answer);
				result = true;
			}
		return result;
	}

	/**
	 * runs an shell command on the remote instance. the executed command is "/etc/init.d/cassandra command" 
	 * @param command start, stop, restart
	 * @return true if succeeded
	 * @throws IOException
	 */
	public boolean cassandraDevice(String command) throws IOException {
		boolean result = false;

		
		SshClient sshClient = connectSSH();
//		PublicKeyAuthenticationClient authClient = new PublicKeyAuthenticationClient();
//
//		// connect to the instance
//		SshPrivateKeyFile sshPrivKeyFile = SshPrivateKeyFile.parse(keyFile);
//		SshPrivateKey sshPrivKey = sshPrivKeyFile.toPrivateKey("");
//		authClient.setKey(sshPrivKey);
//		authClient.setUsername("ubuntu");
//
//		SshClient sshClient = new SshClient();
//		sshClient.connect(instance.getPublicIp(),
//				new IgnoreHostKeyVerification());
//		int authResult = sshClient.authenticate(authClient);
//
//		if (authResult == AuthenticationProtocolState.COMPLETE) {

			SessionChannelClient session = sshClient.openSessionChannel();
			ChannelOutputStream out;

			session.requestPseudoTerminal("gogrid", 80, 24, 0, 0, "");
			if (session.startShell()) {
				out = session.getOutputStream();
				try {
					TimeUnit.SECONDS.sleep(5);
					out.write(("sudo /etc/init.d/cassandra " + command + "\n").getBytes());
					_log.info("sudo /etc/init.d/cassandra " + command);
					TimeUnit.SECONDS.sleep(30);
					result = true;
				} catch (InterruptedException e) {
					e.printStackTrace();
					result = false;
				}
			}
			return result;
		}
}
