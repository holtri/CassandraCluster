package com.hekabe.cassandra.cluster;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.hekabe.cassandra.instance.CassandraInstance;

public abstract class AbstractCassandraCluster {
	
	abstract public List<String> getPublicIps();
	
	abstract public Collection<? extends CassandraInstance> getInstances();
	
	public void setUpBenchmarkConfiguration() {
		ArrayList<CassandraInstance> instances = new ArrayList<CassandraInstance>();
		instances.addAll(getInstances());
		CassandraInstance instance = instances.get(0);

		TTransport tr = new TFramedTransport(new TSocket(
				instance.getPublicIp(), 9160));
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		try {
			tr.open();
			// create keyspace
			String cql = "CREATE KEYSPACE usertable WITH strategy_class = 'org.apache.cassandra.locator.SimpleStrategy' AND strategy_options:replication_factor = 1;"; // create
																																										// usertable
																																										// keyspace
			client.execute_cql_query(ByteBuffer.wrap(cql.getBytes()),
					Compression.NONE);
			// create column family
			cql = "USE usertable;";
			client.execute_cql_query(ByteBuffer.wrap(cql.getBytes()),
					Compression.NONE);
			cql = "CREATE COLUMNFAMILY data (KEY text PRIMARY KEY);";
			;
			client.execute_cql_query(ByteBuffer.wrap(cql.getBytes()),
					Compression.NONE);

			tr.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (SchemaDisagreementException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
