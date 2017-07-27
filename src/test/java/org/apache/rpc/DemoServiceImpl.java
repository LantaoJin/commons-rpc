package org.apache.rpc;

import java.io.IOException;

public class DemoServiceImpl implements DemoService {

	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		return 1;
	}

	@Override
	public int sum(int a, int b) {
		return a + b;
	}

}
