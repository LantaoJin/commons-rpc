package org.apache.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.conf.Configurable;
import org.apache.conf.Configuration;
import org.apache.io.ObjectWritable;
import org.apache.io.UTF8;
import org.apache.io.Writable;
import org.apache.ipc.Client.ConnectionId;
import org.apache.net.NetUtils;

public class RPC {
	
	private static final Log LOG =
		    LogFactory.getLog(RPC.class);
	/**
	 * A method invocation,including the method name and its parametes
	 * @author Administratorr
	 *
	 */
	public static class Invocation implements Writable,Configurable{

		private String methodName;
		private Class[] parameterClasses;
		private Object[] parameters;
		private Configuration conf;
		
		public Invocation(){};
		
		public Invocation(Method method,Object[] parameters){
			this.methodName = method.getName();
			this.parameterClasses = method.getParameterTypes();
			this.parameters = parameters;
		}
		
		
		
		public String getMethodName() {
			return methodName;
		}

		public void setMethodName(String methodName) {
			this.methodName = methodName;
		}

		public Class[] getParameterClasses() {
			return parameterClasses;
		}

		public void setParameterClasses(Class[] parameterClasses) {
			this.parameterClasses = parameterClasses;
		}

		public Object[] getParameters() {
			return parameters;
		}

		public void setParameters(Object[] parameters) {
			this.parameters = parameters;
		}

		public String toString() {
		    StringBuffer buffer = new StringBuffer();
		    buffer.append(methodName);
		    buffer.append("(");
		    for (int i = 0; i < parameters.length; i++) {
		      if (i != 0)
		        buffer.append(", ");
		        buffer.append(parameters[i]);
		     }
		     buffer.append(")");
		     return buffer.toString();
		}
		
		@Override
		public void setConf(Configuration conf) {
			this.conf = conf;
			
		}

		@Override
		public Configuration getConf() {
			// TODO Auto-generated method stub
			return this.conf;
		}

		
		public void readFields(DataInput in) throws IOException {
			methodName = UTF8.readString(in);
			parameters = new Object[in.readInt()];
			parameterClasses = new Class[parameters.length];
			ObjectWritable objectWritable = new ObjectWritable();
			for(int i = 0; i < parameters.length; i++){
				parameters[i] = ObjectWritable.readObject(in, objectWritable, conf);
				parameterClasses[i] = objectWritable.getDeclaredClass();
			}
		}
		
		public void write(DataOutput out) throws IOException {
			UTF8.writeString(out, methodName);
			out.writeInt(parameterClasses.length);
			for(int i = 0; i < parameterClasses.length; i++){
				ObjectWritable.writeObject(out, parameters[i], parameterClasses[i], conf);
			}
		}
	}

	private static class Invoker implements InvocationHandler {
		private Client.ConnectionId remoteId;
		private Client client;
		private boolean isClosed = false;
		
		public Invoker(Class<?> protocol,
				InetSocketAddress address, Configuration conf,
				SocketFactory factory, int rpcTimeout) {
			this.remoteId = ConnectionId.getConnectionId(address,
					protocol, rpcTimeout, conf);
			this.client = CLIENTS.getClient(conf, factory);
		}
		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
			final boolean logDebug = LOG.isDebugEnabled();
			long startTime = 0;
			if (logDebug) {
			    startTime = System.currentTimeMillis();
			}

			 ObjectWritable value = (ObjectWritable)
			    client.call(new Invocation(method, args), remoteId);
			 if (logDebug) {
			    long callTime = System.currentTimeMillis() - startTime;
			    LOG.debug("Call: " + method.getName() + " " + callTime);
			  }
			  return value.get();
			  }
			    
			    /* close the IPC client that's responsible for this invoker's RPCs */ 
			    synchronized private void close() {
			      if (!isClosed) {
			        isClosed = true;
			        CLIENTS.stopClient(client);
			      }
			    }

	}
	
	
	/* Cache a client using its socket factory as the hash key */
	  static private class ClientCache {
	    private Map<SocketFactory, Client> clients =
	      new HashMap<SocketFactory, Client>();

	    /**
	     * Construct & cache an IPC client with the user-provided SocketFactory 
	     * if no cached client exists.
	     * 
	     * @param conf Configuration
	     * @return an IPC client
	     */
	    private synchronized Client getClient(Configuration conf,
	        SocketFactory factory) {
	      // Construct & cache client.  The configuration is only used for timeout,
	      // and Clients have connection pools.  So we can either (a) lose some
	      // connection pooling and leak sockets, or (b) use the same timeout for all
	      // configurations.  Since the IPC is usually intended globally, not
	      // per-job, we choose (a).
	      Client client = clients.get(factory);
	      if (client == null) {
	        client = new Client(ObjectWritable.class, conf, factory);
	        clients.put(factory, client);
	      } else {
	        client.incCount();
	      }
	      return client;
	    }

	    /**
	     * Construct & cache an IPC client with the default SocketFactory 
	     * if no cached client exists.
	     * 
	     * @param conf Configuration
	     * @return an IPC client
	     */
	    private synchronized Client getClient(Configuration conf) {
	      return getClient(conf, SocketFactory.getDefault());
	    }

	    /**
	     * Stop a RPC client connection 
	     * A RPC client is closed only when its reference count becomes zero.
	     */
	    private void stopClient(Client client) {
	      synchronized (this) {
	        client.decCount();
	        if (client.isZeroReference()) {
	          clients.remove(client.getSocketFactory());
	        }
	      }
	      if (client.isZeroReference()) {
	        client.stop();
	      }
	    }
	  }
	private static ClientCache CLIENTS=new ClientCache();
	  
	  //for unit testing only
	  static Client getClient(Configuration conf) {
	    return CLIENTS.getClient(conf);
	  }
	  
	  
	 
	 

	  /** Construct a client-side proxy object that implements the named protocol,
	   * talking to a server at the named address. */
	  public static ProtocolInterface getProxy(
	      Class<? extends ProtocolInterface> protocol,
	      long clientVersion, InetSocketAddress addr,
	      Configuration conf, SocketFactory factory, int rpcTimeout) throws IOException {

	    ProtocolInterface proxy =
	        (ProtocolInterface) Proxy.newProxyInstance(
	            protocol.getClassLoader(), new Class[] { protocol },
	            new Invoker(protocol, addr, conf, factory, rpcTimeout));
//	     serverVersion = proxy.getProtocolVersion(protocol.getName(), clientVersion);
	    /*if (serverVersion == clientVersion) {
	      return proxy;
	    } else {
	      throw new VersionMismatch(protocol.getName(), clientVersion, 
	                                serverVersion);
	    }*/
	    return proxy;
//	    return null;
	  }
	/**
	   * Construct a client-side proxy object with the default SocketFactory
	   * 
	   * @param protocol
	   * @param clientVersion
	   * @param addr
	   * @param conf
	   * @return a proxy instance
	   * @throws IOException
	   */
	  public static ProtocolInterface getProxy(
	      Class<? extends ProtocolInterface> protocol,
	      long clientVersion, InetSocketAddress addr, Configuration conf)
	      throws IOException {
	    return getProxy(protocol, clientVersion, addr, conf,
	        NetUtils.getDefaultSocketFactory(conf), 0);
	  }

	  public static ProtocolInterface getProxy(
	      Class<? extends ProtocolInterface> protocol,
	      long clientVersion, InetSocketAddress addr, Configuration conf, int rpcTimeout)
	      throws IOException {

	    return getProxy(protocol, clientVersion, addr, conf,
	        NetUtils.getDefaultSocketFactory(conf), rpcTimeout);
	  }

	  /**
	   * Stop this proxy and release its invoker's resource
	   * @param proxy the proxy to be stopped
	   */
	  public static void stopProxy(ProtocolInterface proxy) {
	    if (proxy!=null) {
	      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
	    }
	  }
	  
	  /** Construct a server for a protocol implementation instance listening on a
	   * port and address. */
	  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf) 
	    throws IOException {
	    return getServer(instance, bindAddress, port, 1, false, conf);
	  }

	  

	  /** Construct a server for a protocol implementation instance listening on a
	   * port and address */
	  public static Server getServer(final Object instance, final String bindAddress, final int port,
	                                 final int numHandlers,
	                                 final boolean verbose, Configuration conf) 
	    throws IOException {
	    return new Server(instance, conf, bindAddress, port, numHandlers, verbose);
	  }
	  

	  
	  /** An RPC Server. */
	  public static class Server extends org.apache.ipc.Server {
	    private Object instance;
	    private boolean verbose;

	    /** Construct an RPC server.
	     * @param instance the instance whose methods will be called
	     * @param conf the configuration to use
	     * @param bindAddress the address to bind on to listen for connection
	     * @param port the port to listen for connections on
	     */
	    public Server(Object instance, Configuration conf, String bindAddress, int port) 
	      throws IOException {
	      this(instance, conf,  bindAddress, port, 1, false);
	    }
	    
	    private static String classNameBase(String className) {
	      String[] names = className.split("\\.", -1);
	      if (names == null || names.length == 0) {
	        return className;
	      }
	      return names[names.length-1];
	    }
	    
	    /** Construct an RPC server.
	     * @param instance the instance whose methods will be called
	     * @param conf the configuration to use
	     * @param bindAddress the address to bind on to listen for connection
	     * @param port the port to listen for connections on
	     * @param numHandlers the number of method handler threads to run
	     * @param verbose whether each call should be logged
	     */
	    public Server(Object instance, Configuration conf, String bindAddress,  int port,
	                  int numHandlers, boolean verbose) 
	        throws IOException {
	      super(bindAddress, port, Invocation.class, numHandlers, conf);
	      this.instance = instance;
	      this.verbose = verbose;
	    }

	    public Writable call(Class<?> protocol, Writable param, long receivedTime) 
	    throws IOException {
	      try {
	        Invocation call = (Invocation)param;
	        if (verbose) log("Call: " + call);

	        Method method =
	          protocol.getMethod(call.getMethodName(),
	                                   call.getParameterClasses());
	        method.setAccessible(true);

	        long startTime = System.currentTimeMillis();
	        Object value = method.invoke(instance, call.getParameters());
	        int processingTime = (int) (System.currentTimeMillis() - startTime);
	        int qTime = (int) (startTime-receivedTime);
	        if (LOG.isDebugEnabled()) {
	          LOG.debug("Served: " + call.getMethodName() +
	                    " queueTime= " + qTime +
	                    " procesingTime= " + processingTime);
	        }
//	        rpcMetrics.addRpcQueueTime(qTime);
//	        rpcMetrics.addRpcProcessingTime(processingTime);
//	        rpcMetrics.addRpcProcessingTime(call.getMethodName(), processingTime);
	        if (verbose) log("Return: "+value);

	        return new ObjectWritable(method.getReturnType(), value);

	      } catch (InvocationTargetException e) {
	        Throwable target = e.getTargetException();
	        if (target instanceof IOException) {
	          throw (IOException)target;
	        } else {
	          IOException ioe = new IOException(target.toString());
	          ioe.setStackTrace(target.getStackTrace());
	          throw ioe;
	        }
	      } catch (Throwable e) {
	        if (!(e instanceof IOException)) {
	          LOG.error("Unexpected throwable object ", e);
	        }
	        IOException ioe = new IOException(e.toString());
	        ioe.setStackTrace(e.getStackTrace());
	        throw ioe;
	      }
	    }
	  }
	  
	  private static void log(String value) {
		    if (value!= null && value.length() > 55)
		      value = value.substring(0, 55)+"...";
		    LOG.info(value);
      }
}
