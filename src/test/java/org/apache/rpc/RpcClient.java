package org.apache.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.conf.Configuration;
import org.apache.ipc.RPC;

public class RpcClient {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		DemoService proxy = (DemoService) RPC.getProxy(DemoService.class, DemoService.versionID,
				new InetSocketAddress(conf.get("client.ip.name"), conf.getInt("name.port", 8888)), conf, 1000);
		System.out.println("client receive:" + proxy.sum(100, 68));
		System.out.println("client receive:" + proxy.sum(888, 666));
		RPC.stopProxy(proxy);
	}

}
