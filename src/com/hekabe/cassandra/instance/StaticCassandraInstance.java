package com.hekabe.cassandra.instance;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.SshException;
import com.sshtools.j2ssh.authentication.AuthenticationProtocolState;
import com.sshtools.j2ssh.authentication.PasswordAuthenticationClient;
import com.sshtools.j2ssh.transport.IgnoreHostKeyVerification;

public class StaticCassandraInstance extends CassandraInstance {

	private static Logger _log = Logger.getLogger(StaticCassandraInstance.class);

	private String publicIp;
	private String username;
	private String password;
	
	public StaticCassandraInstance (String publicIp, String username, String password){
		this.publicIp = publicIp;
		this.username = username;
		this.password = password;
	}
	
	@Override
	public boolean isInstanceRunning() {
//		boolean result = false;
//		InetAddress address;
//		try {
//			address = InetAddress.getByName(publicIp);
//			result = address.isReachable(1000);
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return result;
		return true;
	}

	@Override
	public String getPublicIp() {
		return this.publicIp;
	}

	@Override
	public String getPrivateIp() {
		return this.getPublicIp();
	}

	@Override
	public SshClient connectSSH() throws IOException {
		PasswordAuthenticationClient authClient = new PasswordAuthenticationClient();
		authClient.setUsername(username);
		authClient.setPassword(password);
		
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
