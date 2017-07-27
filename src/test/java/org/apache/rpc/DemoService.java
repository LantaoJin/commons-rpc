package org.apache.rpc;
import org.apache.ipc.ProtocolInterface;


public interface DemoService extends ProtocolInterface {
	
	public static final long versionID = 1L;
	
	public int sum(int a, int b);
	
}
