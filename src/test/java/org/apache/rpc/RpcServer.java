package org.apache.rpc;

import org.apache.conf.Configuration;
import org.apache.ipc.RPC;
import org.apache.ipc.Server;

public class RpcServer {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Server server = RPC.getServer(new DemoServiceImpl(), conf.get("server.ip.name"), conf.getInt("name:port", 8888),
				new Configuration());
		server.start();
		// server.stop();
	}
}
