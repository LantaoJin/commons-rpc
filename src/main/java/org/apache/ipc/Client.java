package org.apache.ipc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.conf.Configuration;
import org.apache.io.DataOutputBuffer;
import org.apache.io.IOUtils;
import org.apache.io.ObjectWritable;
import org.apache.io.Writable;
import org.apache.io.WritableUtils;
import org.apache.net.NetUtils;
import org.apache.util.ReflectionUtils;

public class Client {

	public static final Log LOG = LogFactory.getLog(Client.class);
	final static int PING_CALL_ID = -1;
	private Hashtable<ConnectionId, Connection> connections = new Hashtable<ConnectionId, Connection>();

	private int counter; // counter for call ids
	private SocketFactory socketFactory; // how to create sockets
	private Class<? extends Writable> valueClass; // class of call values
	final private Configuration conf;
	private int refCount = 1;
	private AtomicBoolean running = new AtomicBoolean(true); // if client runs

	/**
	 * Get a connection from the pool, or create a new one and add it to the
	 * pool. Connections to a given ConnectionId are reused.
	 */
	private Connection getConnection(ConnectionId remoteId, Call call) throws IOException, InterruptedException {
		if (!running.get()) {
			// the client is stopped
			throw new IOException("The client is stopped");
		}
		Connection connection;
		/*
		 * we could avoid this allocation for each RPC by having a connectionsId
		 * object and with set() method. We need to manage the refs for keys in
		 * HashMap properly. For now its ok.
		 */
		do {
			synchronized (connections) {
				connection = connections.get(remoteId);
				if (connection == null) {
					connection = new Connection(remoteId);
					connections.put(remoteId, connection);
				}
			}
		} while (!connection.addCall(call));

		// we don't invoke the method below inside "synchronized (connections)"
		// block above. The reason for that is if the server happens to be slow,
		// it will take longer to establish a connection and that will slow the
		// entire system down.
		connection.setupIOstreams();
		return connection;
	}

	/**
	 * This class holds the address.The client connections to servers are
	 * uniquely identified by <remoteAddress, protocol>
	 */
	static class ConnectionId {
		InetSocketAddress address;
		Class<?> protocol;
		private int rpcTimeout;
		private int maxIdleTime; // connections will be culled if it was idle
									// for
		// maxIdleTime msecs;
		private int maxRetries; // the max. no. of retries for socket
								// connections
		private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
		private int pingInterval; // how often sends ping to the server in mses;

		private static final int PRIME = 16777619;

		public ConnectionId(InetSocketAddress address, Class<?> protocol, int rpcTimeout, int maxIdleTime,
				int maxRetries, boolean tcpNoDelay, int pingInterval) {
			this.address = address;
			this.protocol = protocol;
			this.rpcTimeout = rpcTimeout;
			this.maxIdleTime = maxIdleTime;
			this.maxRetries = maxRetries;
			this.tcpNoDelay = tcpNoDelay;
			this.pingInterval = pingInterval;
		}

		public InetSocketAddress getAddress() {
			return address;
		}

		public void setAddress(InetSocketAddress address) {
			this.address = address;
		}

		public Class<?> getProtocol() {
			return protocol;
		}

		public void setProtocol(Class<?> protocol) {
			this.protocol = protocol;
		}

		public int getRpcTimeout() {
			return rpcTimeout;
		}

		public void setRpcTimeout(int rpcTimeout) {
			this.rpcTimeout = rpcTimeout;
		}

		public int getMaxIdleTime() {
			return maxIdleTime;
		}

		public void setMaxIdleTime(int maxIdleTime) {
			this.maxIdleTime = maxIdleTime;
		}

		public int getMaxRetries() {
			return maxRetries;
		}

		public void setMaxRetries(int maxRetries) {
			this.maxRetries = maxRetries;
		}

		public boolean getTcpNoDelay() {
			return tcpNoDelay;
		}

		public void setTcpNoDelay(boolean tcpNoDelay) {
			this.tcpNoDelay = tcpNoDelay;
		}

		public int getPingInterval() {
			return pingInterval;
		}

		public void setPingInterval(int pingInterval) {
			this.pingInterval = pingInterval;
		}

		public static ConnectionId getConnectionId(InetSocketAddress addr, Class<?> protocol, Configuration conf) {
			return getConnectionId(addr, protocol, 0, conf);
		}

		public static ConnectionId getConnectionId(InetSocketAddress addr, Class<?> protocol, int rpcTimeout,
				Configuration conf) {
			return new ConnectionId(addr, protocol, rpcTimeout, conf.getInt("ipc.client.connection.maxidletime", 10000), // 10s
					conf.getInt("ipc.client.connect.max.retries", 10), conf.getBoolean("ipc.client.tcpnodelay", false),
					Client.getPingInterval(conf));
		}

	}

	final private static String PING_INTERVAL_NAME = "ipc.ping.interval";
	final static int DEFAULT_PING_INTERVAL = 60000; // 1 min

	/**
	 * set the ping interval value in configuration
	 * 
	 * @param conf
	 *            Configuration
	 * @param pingInterval
	 *            the ping interval
	 */
	final public static void setPingInterval(Configuration conf, int pingInterval) {
		conf.setInt(PING_INTERVAL_NAME, pingInterval);
	}

	/**
	 * Get the ping interval from configuration; If not set in the
	 * configuration, return the default value.
	 * 
	 * @param conf
	 *            Configuration
	 * @return the ping interval
	 */
	final static int getPingInterval(Configuration conf) {
		return conf.getInt(PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL);
	}

	/**
	 * Increment this client's reference count
	 *
	 */
	synchronized void incCount() {
		refCount++;
	}

	/**
	 * Decrement this client's reference count
	 *
	 */
	synchronized void decCount() {
		refCount--;
	}

	/**
	 * Return if this client has no reference
	 * 
	 * @return true if this client has no reference; false otherwise
	 */
	synchronized boolean isZeroReference() {
		return refCount == 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * Thread that reads responses and notifiers callers. Each connection owns a
	 * socket connected to a remote address. Calls are multiplexed through this
	 * socket :responses may be delivered out of order.
	 */
	private class Connection extends Thread {
		private InetSocketAddress server; // server ip:port
		private ConnectionHeader header;
		private final ConnectionId remoteId;

		private Socket socket = null; // connected socket
		private DataInputStream in;
		private DataOutputStream out;

		private int rpcTimeout;
		private int maxIdletime;
		private int maxRetries;
		private boolean tcpNoDelay;
		private int pingInterval;

		// currently acitve calls
		private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
		private AtomicLong lastActivity = new AtomicLong();// last I/O activity
															// time
		private AtomicBoolean shouldCloseConnection = new AtomicBoolean(); // indicate
																			// if
																			// the
																			// connection
																			// is
																			// close
		private IOException closeException;// close reason;

		public Connection(ConnectionId remoteId) throws IOException {
			this.remoteId = remoteId;
			this.server = remoteId.getAddress();
			if (server.isUnresolved()) {
				throw new UnknownHostException("unknown host:" + remoteId.getAddress().getHostName());
			}
			this.maxIdletime = remoteId.getMaxIdleTime();
			this.maxRetries = remoteId.getMaxRetries();
			this.tcpNoDelay = remoteId.getTcpNoDelay();
			this.pingInterval = remoteId.getPingInterval();
			if (LOG.isDebugEnabled()) {
				LOG.debug("This ping interval is " + this.pingInterval + "ms.");
			}
			this.rpcTimeout = remoteId.getRpcTimeout();
			Class<?> protocol = remoteId.getProtocol();

			header = new ConnectionHeader(protocol == null ? null : protocol.getName());

			this.setName("IPC Client (" + socketFactory.hashCode() + ") connection to "
					+ remoteId.getAddress().toString() + ")");
			this.setDaemon(true);

		}

		/** Update lastActivity with the current time. */
		private void touch() {
			lastActivity.set(System.currentTimeMillis());
		}

		private synchronized boolean addCall(Call call) {
			if (shouldCloseConnection.get()) {
				return false;
			}
			calls.put(call.id, call);
			notify();
			return true;
		}

		/**
		 * This class sends a ping to the remote side when timeout on reading.
		 * If no failure is detected, it retries until at least a byte is read.
		 */
		private class PingInputStream extends FilterInputStream {
			/* constructor */
			protected PingInputStream(InputStream in) {
				super(in);
			}

			/*
			 * Process timeout exception if the connection is not going to be
			 * closed or is not configured to have a RPC timeout, send a ping.
			 * (if rpcTimeout is not set to be 0, then RPC should timeout.
			 * otherwise, throw the timeout exception.
			 */
			private void handleTimeout(SocketTimeoutException e) throws IOException {
				if (shouldCloseConnection.get() || !running.get() || rpcTimeout > 0) {
					throw e;
				} else {
					sendPing();
				}
			}

			/**
			 * Read a byte from the stream. Send a ping if timeout on read.
			 * Retries if no failure is detected until a byte is read.
			 * 
			 * @throws IOException
			 *             for any IO problem other than socket timeout
			 */
			@Override
			public int read() throws IOException {
				do {
					try {
						return super.read();
					} catch (SocketTimeoutException e) {
						handleTimeout(e);
					}
				} while (true);
			}

			/**
			 * Read bytes into a buffer starting from offset <code>off</code>
			 * Send a ping if timeout on read. Retries if no failure is detected
			 * until a byte is read.
			 * 
			 * @return the total number of bytes read; -1 if the connection is
			 *         closed.
			 */
			@Override
			public int read(byte[] buf, int off, int len) throws IOException {
				do {
					try {
						return super.read(buf, off, len);
					} catch (SocketTimeoutException e) {
						handleTimeout(e);
					}
				} while (true);
			}
		}

		/*
		 * Send a ping to the server if the time elapsed since last I/O activity
		 * is equal to or greater than the ping interval
		 */
		private synchronized void sendPing() throws IOException {
			long curTime = System.currentTimeMillis();
			if (curTime - lastActivity.get() >= pingInterval) {
				lastActivity.set(curTime);
				synchronized (out) {
					out.writeInt(PING_CALL_ID);
					out.flush();
				}
			}
		}

		@Override
		public void run() {
			if (LOG.isDebugEnabled())
				LOG.debug(getName() + ": starting, having connections " + connections.size());

			while (waitForWork()) {// wait here for work - read or close
									// connection
				receiveResponse();
			}

			close();

			if (LOG.isDebugEnabled())
				LOG.debug(getName() + ": stopped, remaining connections " + connections.size());
		}

		/**
		 * Initiates a call by sending the parameter to the remote server. Note:
		 * this is not called from the Connection thread, but by other threads.
		 */
		public void sendParam(Call call) {
			if (shouldCloseConnection.get()) {
				return;
			}

			DataOutputBuffer d = null;
			try {
				synchronized (this.out) {
					if (LOG.isDebugEnabled())
						LOG.debug(getName() + " sending #" + call.id);

					// for serializing the
					// data to be written
					d = new DataOutputBuffer();
					d.writeInt(call.id);
					call.param.write(d);
					byte[] data = d.getData();
					int dataLength = d.getLength();
					out.writeInt(dataLength); // first put the data length
					out.write(data, 0, dataLength);// write the data
					out.flush();
				}
			} catch (IOException e) {
				markClosed(e);
			} finally {
				// the buffer is just an in-memory buffer, but it is still
				// polite to
				// close early
				IOUtils.closeStream(d);
			}
		}

		/*
		 * Receive a response. Because only one receiver, so no synchronization
		 * on in.
		 */
		private void receiveResponse() {
			if (shouldCloseConnection.get()) {
				return;
			}
			touch();

			try {
				int id = in.readInt(); // try to read an id

				if (LOG.isDebugEnabled())
					LOG.debug(getName() + " got value #" + id);

				Call call = calls.get(id);

				int state = in.readInt(); // read call status
				if (state == Status.SUCCESS.state) {
					Writable value = ReflectionUtils.newInstance(valueClass, conf);
					((ObjectWritable) value).setConf(conf);
					value.readFields(in); // read value
					call.setValue(value);
					calls.remove(id);
				} else if (state == Status.ERROR.state) {
					call.setException(new RemoteException(WritableUtils.readString(in), WritableUtils.readString(in)));
					calls.remove(id);
				} else if (state == Status.FATAL.state) {
					// Close the connection
					markClosed(new RemoteException(WritableUtils.readString(in), WritableUtils.readString(in)));
				}
			} catch (IOException e) {
				markClosed(e);
			}
		}

		private synchronized void markClosed(IOException e) {
			if (shouldCloseConnection.compareAndSet(false, true)) {
				closeException = e;
				notifyAll();
			}
		}

		/**
		 * Update the server address if the address corresponding to the host
		 * name has changed.
		 *
		 * @return true if an addr change was detected.
		 * @throws IOException
		 *             when the hostname cannot be resolved.
		 */
		private synchronized boolean updateAddress() throws IOException {
			// Do a fresh lookup with the old host name.
			InetSocketAddress currentAddr = NetUtils.makeSocketAddr(server.getHostName(), server.getPort());

			if (!server.equals(currentAddr)) {
				LOG.warn("Address change detected. Old: " + server.toString() + " New: " + currentAddr.toString());
				server = currentAddr;
				return true;
			}
			return false;
		}

		private synchronized void setupConnection() throws IOException {
			short ioFailures = 0;
			short timeoutFailures = 0;
			while (true) {
				try {
					this.socket = socketFactory.createSocket();
					this.socket.setTcpNoDelay(tcpNoDelay);
					/*
					 * Bind the socket to the host specified in the principal
					 * name of the client, to ensure Server matching address of
					 * the client connection to host name in principal passed.
					 */
					/*
					 * if (UserGroupInformation.isSecurityEnabled()) {
					 * KerberosInfo krbInfo =
					 * remoteId.getProtocol().getAnnotation(KerberosInfo.class);
					 * if (krbInfo != null && krbInfo.clientPrincipal() != null)
					 * { String host =
					 * SecurityUtil.getHostFromPrincipal(remoteId.getTicket().
					 * getUserName());
					 * 
					 * // If host name is a valid local address then bind socket
					 * to it InetAddress localAddr =
					 * NetUtils.getLocalInetAddress(host); if (localAddr !=
					 * null) { this.socket.bind(new InetSocketAddress(localAddr,
					 * 0)); } } }
					 */
					// connection time out is 20s

					this.socket.bind(new InetSocketAddress(conf.get("client.ip.name"), 0));
					NetUtils.connect(this.socket, server, 20000);
					if (rpcTimeout > 0) {
						pingInterval = rpcTimeout; // rpcTimeout overwrites
													// pingInterval
					}

					this.socket.setSoTimeout(pingInterval);
					return;
				} catch (SocketTimeoutException toe) {
					/*
					 * Check for an address change and update the local
					 * reference. Reset the failure counter if the address was
					 * changed
					 */
					if (updateAddress()) {
						timeoutFailures = ioFailures = 0;
					}
					/*
					 * The max number of retries is 45, which amounts to 20s*45
					 * = 15 minutes retries.
					 */
					handleConnectionFailure(timeoutFailures++, 45, toe);
				} catch (IOException ie) {
					if (updateAddress()) {
						timeoutFailures = ioFailures = 0;
					}
					handleConnectionFailure(ioFailures++, maxRetries, ie);
				}
			}
		}

		/** Close the connection. */
		private synchronized void close() {
			if (!shouldCloseConnection.get()) {
				LOG.error("The connection is not in the closed state");
				return;
			}

			// release the resources
			// first thing to do;take the connection out of the connection list
			synchronized (connections) {
				if (connections.get(remoteId) == this) {
					connections.remove(remoteId);
				}
			}

			// close the streams and therefore the socket
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);

			// clean up all calls
			if (closeException == null) {
				if (!calls.isEmpty()) {
					LOG.warn("A connection is closed for no cause and calls are not empty");

					// clean up calls anyway
					closeException = new IOException("Unexpected closed connection");
					cleanupCalls();
				}
			} else {
				// log the info
				if (LOG.isDebugEnabled()) {
					LOG.debug("closing ipc connection to " + server + ": " + closeException.getMessage(),
							closeException);
				}

				// cleanup calls
				cleanupCalls();
			}
			if (LOG.isDebugEnabled())
				LOG.debug(getName() + ": closed");
		}

		/* Cleanup all calls and mark them as done */
		private void cleanupCalls() {
			Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
			while (itor.hasNext()) {
				Call c = itor.next().getValue();
				c.setException(closeException); // local exception
				itor.remove();
			}
		}

		/**
		 * Connect to the server and set up the I/O streams. It then sends a
		 * header to the server and starts the connection thread that waits for
		 * responses.
		 */
		private synchronized void setupIOstreams() throws InterruptedException {
			if (socket != null || shouldCloseConnection.get()) {
				return;
			}

			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Connecting to " + server);
				}
				/*
				 * short numRetries = 0; final short maxRetries = 15; Random
				 * rand = null;
				 */
				while (true) {
					setupConnection();
					InputStream inStream = NetUtils.getInputStream(socket);
					OutputStream outStream = NetUtils.getOutputStream(socket);
					writeRpcHeader(outStream);
					/*
					 * if (useSasl) { final InputStream in2 = inStream; final
					 * OutputStream out2 = outStream; UserGroupInformation
					 * ticket = remoteId.getTicket(); if (authMethod ==
					 * AuthMethod.KERBEROS) { if (ticket.getRealUser() != null)
					 * { ticket = ticket.getRealUser(); } } boolean continueSasl
					 * = false; try { continueSasl = ticket.doAs(new
					 * PrivilegedExceptionAction<Boolean>() {
					 * 
					 * @Override public Boolean run() throws IOException {
					 * return setupSaslConnection(in2, out2); } }); } catch
					 * (Exception ex) { if (rand == null) { rand = new Random();
					 * } handleSaslConnectionFailure(numRetries++, maxRetries,
					 * ex, rand, ticket); continue; } if (continueSasl) { //
					 * Sasl connect is successful. Let's set up Sasl i/o
					 * streams. inStream =
					 * saslRpcClient.getInputStream(inStream); outStream =
					 * saslRpcClient.getOutputStream(outStream); } else { //
					 * fall back to simple auth because server told us so.
					 * authMethod = AuthMethod.SIMPLE; header = new
					 * ConnectionHeader(header.getProtocol(), header.getUgi(),
					 * authMethod); useSasl = false; } }
					 */
					this.in = new DataInputStream(new BufferedInputStream(new PingInputStream(inStream)));
					this.out = new DataOutputStream(new BufferedOutputStream(outStream));
					writeHeader();

					// update last activity time
					touch();

					// start the receiver thread after the socket connection has
					// been set up
					start();
					return;
				}
			} catch (Throwable t) {
				if (t instanceof IOException) {
					markClosed((IOException) t);
				} else {
					markClosed(new IOException("Couldn't set up IO streams", t));
				}
				close();
			}
		}

		private void closeConnection() {
			// close the current connection
			try {
				socket.close();
			} catch (IOException e) {
				LOG.warn("Not able to close a socket", e);
			}
			// set socket to null so that the next call to setupIOstreams
			// can start the process of connect all over again.
			socket = null;
		}

		/*
		 * Handle connection failures
		 *
		 * If the current number of retries is equal to the max number of
		 * retries, stop retrying and throw the exception; Otherwise backoff 1
		 * second and try connecting again.
		 *
		 * This Method is only called from inside setupIOstreams(), which is
		 * synchronized. Hence the sleep is synchronized; the locks will be
		 * retained.
		 *
		 * @param curRetries current number of retries
		 * 
		 * @param maxRetries max number of retries allowed
		 * 
		 * @param ioe failure reason
		 * 
		 * @throws IOException if max number of retries is reached
		 */
		private void handleConnectionFailure(int curRetries, int maxRetries, IOException ioe) throws IOException {

			closeConnection();

			// throw the exception if the maximum number of retries is reached
			if (curRetries >= maxRetries) {
				throw ioe;
			}

			// otherwise back off and retry
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ignored) {
			}

			LOG.info("Retrying connect to server: " + server + ". Already tried " + curRetries + " time(s).");
		}

		/* Write the RPC header */
		private void writeRpcHeader(OutputStream outStream) throws IOException {
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
			// Write out the header, version and authentication method
			out.write(Server.HEADER.array());
			out.write(Server.CURRENT_VERSION);
			// authMethod.write(out);
			out.flush();
		}

		/*
		 * Write the protocol header for each connection Out is not synchronized
		 * because only the first thread does this.
		 */
		private void writeHeader() throws IOException {
			// Write out the ConnectionHeader
			DataOutputBuffer buf = new DataOutputBuffer();
			header.write(buf);

			// Write out the payload length
			int bufLen = buf.getLength();
			out.writeInt(bufLen);
			out.write(buf.getData(), 0, bufLen);
		}

		/*
		 * wait till someone signals us to start reading RPC response or it is
		 * idle too long, it is marked as to be closed, or the client is marked
		 * as not running.
		 * 
		 * Return true if it is time to read a response; false otherwise.
		 */
		private synchronized boolean waitForWork() {
			if (calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
				long timeout = maxIdletime - (System.currentTimeMillis() - lastActivity.get());
				if (timeout > 0) {
					try {
						wait(timeout);
					} catch (InterruptedException e) {
					}
				}
			}

			if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
				return true;
			} else if (shouldCloseConnection.get()) {
				return false;
			} else if (calls.isEmpty()) { // idle connection closed or stopped
				markClosed(null);
				return false;
			} else { // get stopped but there are still pending requests
				markClosed((IOException) new IOException().initCause(new InterruptedException()));
				return false;
			}
		}

		public InetSocketAddress getRemoteAddress() {
			return server;
		}
	}

	/**
	 * A call waiting for a value.
	 */
	private class Call {
		int id; // call id
		Writable param; // parameter
		Writable value; // value,null if error;
		IOException error; // exception null if value
		boolean done; // true when call is done

		protected Call(Writable param) {
			this.param = param;
			synchronized (Client.this) {
				this.id = counter++;

			}
		}

		/**
		 * Indicate when the call is complete and the value or error are
		 * available. Notifies by default.
		 */
		protected synchronized void callComplete() {
			this.done = true;
			notify(); // notify caller
		}

		/**
		 * Set the exception when there is an error. Notify the caller the call
		 * is done.
		 * 
		 * @param error
		 *            exception thrown by the call; either local or remote
		 */
		public synchronized void setException(IOException error) {
			this.error = error;
			callComplete();
		}

		/**
		 * Set the return value when there is no error. Notify the caller the
		 * call is done.
		 * 
		 * @param value
		 *            return value of the call.
		 */
		public synchronized void setValue(Writable value) {
			this.value = value;
			callComplete();
		}
	}

	/**
	 * Construct an IPC client whose values are of the given {@link Writable}
	 * class.
	 */
	public Client(Class<? extends Writable> valueClass, Configuration conf, SocketFactory factory) {
		this.valueClass = valueClass;
		this.conf = conf;
		this.socketFactory = factory;
	}

	/**
	 * Return the socket factory of this client
	 *
	 * @return this client's socket factory
	 */
	SocketFactory getSocketFactory() {
		return socketFactory;
	}

	/**
	 * Stop all threads related to this client. No further calls may be made
	 * using this client.
	 */
	public void stop() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Stopping client");
		}

		if (!running.compareAndSet(true, false)) {
			return;
		}

		// wake up all connections
		synchronized (connections) {
			for (Connection conn : connections.values()) {
				conn.interrupt();
			}
		}

		// wait until all connections are closed
		while (!connections.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}

	/**
	 * Make a call, passing <code>param</code>, to the IPC server defined by
	 * <code>remoteId</code>, returning the value. Throws exceptions if there
	 * are network problems or if the remote code threw an exception.
	 */
	public Writable call(Writable param, ConnectionId remoteId) throws InterruptedException, IOException {
		Call call = new Call(param);
		Connection connection = getConnection(remoteId, call);
		connection.sendParam(call); // send the parameter
		boolean interrupted = false;
		synchronized (call) {
			while (!call.done) {
				try {
					call.wait(); // wait for the result
				} catch (InterruptedException ie) {
					// save the fact that we were interrupted
					interrupted = true;
				}
			}

			if (interrupted) {
				// set the interrupt flag now that we are done waiting
				Thread.currentThread().interrupt();
			}

			if (call.error != null) {
				if (call.error instanceof RemoteException) {
					call.error.fillInStackTrace();
					throw call.error;
				} else { // local exception
					// use the connection because it will reflect an ip change,
					// unlike
					// the remoteId
					throw wrapException(connection.getRemoteAddress(), call.error);
				}
			} else {
				return call.value;
			}
		}
	}

	/**
	 * Take an IOException and the address we were trying to connect to and
	 * return an IOException with the input exception as the cause. The new
	 * exception provides the stack trace of the place where the exception is
	 * thrown and some extra diagnostics information. If the exception is
	 * ConnectException or SocketTimeoutException, return a new one of the same
	 * type; Otherwise return an IOException.
	 * 
	 * @param addr
	 *            target address
	 * @param exception
	 *            the relevant exception
	 * @return an exception to throw
	 */
	private IOException wrapException(InetSocketAddress addr, IOException exception) {
		if (exception instanceof ConnectException) {
			// connection refused; include the host:port in the error
			return (ConnectException) new ConnectException(
					"Call to " + addr + " failed on connection exception: " + exception).initCause(exception);
		} else if (exception instanceof SocketTimeoutException) {
			return (SocketTimeoutException) new SocketTimeoutException(
					"Call to " + addr + " failed on socket timeout exception: " + exception).initCause(exception);
		} else {
			return (IOException) new IOException("Call to " + addr + " failed on local exception: " + exception)
					.initCause(exception);

		}
	}
}
