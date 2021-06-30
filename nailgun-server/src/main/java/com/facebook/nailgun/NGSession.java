/*
  Copyright 2004-2012, Martian Software, Inc.
  Copyright 2017-Present Facebook, Inc

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.facebook.nailgun;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads the NailGun stream from the client through the command, then hands off processing to the
 * appropriate class. The NGSession obtains its sockets from an NGSessionPool, which created this
 * NGSession.
 *
 * @author <a href="http://www.martiansoftware.com/contact.html">Marty Lamb</a>
 */
public class NGSession extends Thread {

  @FunctionalInterface
  public interface CommnunicatorCreator {
    NGCommunicator get(Socket socket) throws IOException;
  }

  private static final Logger LOG = Logger.getLogger(NGSession.class.getName());

  /** The server this NGSession is working for */
  private final NGServer server;
  /** The pool this NGSession came from, and to which it will return itself */
  private final NGSessionPool sessionPool;
  /** Factory method to provide NGCommunicator to operate on a socket */
  private final CommnunicatorCreator communicatorCreator;
  /** Synchronization object */
  private final Object lock = new Object();
  /** The next socket this NGSession has been tasked with processing (by NGServer) */
  private Socket nextSocket = null;
  /** True if the server has been shutdown and this NGSession should terminate completely */
  private boolean done = false;
  /**
   * The instance number of this NGSession. That is, if this is the Nth NGSession to be created,
   * then this is the value for N.
   */
  private final long instanceNumber;

  /** The interval to wait between heartbeats before considering the client to have disconnected. */
  private final int heartbeatTimeoutMillis;

  /** The instance counter shared among all NGSessions */
  private static AtomicLong instanceCounter = new AtomicLong(0);
  /** signature of main(String[]) for reflection operations */
  private static final Class[] mainSignature = {
    String[].class,
  };
  /** signature of nailMain(NGContext) for reflection operations */
  private static final Class[] nailMainSignature = {
    NGContext.class,
  };

  /** A ClassLoader that may be set by a client. Defaults to the classloader of this class. */
  public static volatile ClassLoader classLoader =
      null; // initialized in the static initializer - see below

  static {
    try {
      classLoader = NGSession.class.getClassLoader();
    } catch (SecurityException e) {
      throw e;
    }
  }

  /**
   * Creates a new NGSession running for the specified NGSessionPool and NGServer.
   *
   * @param sessionPool The NGSessionPool we're working for
   * @param server The NGServer we're working for
   */
  NGSession(NGSessionPool sessionPool, NGServer server) {
    this(sessionPool, server, null);
  }

  /**
   * Creates a new NGSession running for the specified NGSessionPool and NGServer.
   *
   * @param sessionPool The NGSessionPool we're working for
   * @param server The NGServer we're working for
   */
  NGSession(NGSessionPool sessionPool, NGServer server, CommnunicatorCreator communicatorCreator) {
    super();
    this.sessionPool = sessionPool;
    this.server = server;
    this.heartbeatTimeoutMillis = server.getHeartbeatTimeout();
    this.instanceNumber = instanceCounter.incrementAndGet();
    this.communicatorCreator =
        communicatorCreator != null
            ? communicatorCreator
            : (socket -> new NGCommunicator(socket, this.heartbeatTimeoutMillis));
  }

  /**
   * Shuts down this NGSession gracefully by signalling the main nail thread to interrupt. The
   * function exists immediately, it is up to the nail to process interruption and return
   */
  void shutdown() {
    synchronized (lock) {
      done = true;
      nextSocket = null;
      lock.notifyAll();
    }
    interrupt();
  }

  /**
   * Instructs this NGSession to process the specified socket, after which this NGSession will
   * return itself to the pool from which it came.
   *
   * @param socket the socket (connected to a client) to process
   */
  public void run(Socket socket) {
    synchronized (lock) {
      nextSocket = socket;
      lock.notify();
    }
    Thread.yield();
  }

  /**
   * Returns the next socket to process. This will block the NGSession thread until there's a socket
   * to process or the NGSession has been shut down.
   *
   * @return the next socket to process, or <code>null</code> if the NGSession has been shut down.
   */
  private Socket nextSocket() {
    Socket result;
    synchronized (lock) {
      result = nextSocket;
      while (!done && result == null) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          done = true;
        }
        result = nextSocket;
      }
      nextSocket = null;
    }

    if (result != null) {
      // Java InputStream API is blocking by default with no reliable way to stop pending
      // read() call. Setting the timeout to underlying socket will make socket's underlying
      // read() calls throw SocketTimeoutException which unblocks read(). The exception must
      // be properly handled by calling code.
      try {
        // TODO(buck_team): this does not work with current NGUnixDomainSocket
        // implementation
        result.setSoTimeout(this.heartbeatTimeoutMillis);
      } catch (SocketException e) {
        // this exception might be thrown if socket is already closed
        // so we just return null
        return null;
      }
    }

    return result;
  }

  /**
   * The main NGSession loop. This gets the next socket to process, runs the nail for the socket,
   * and loops until shut down.
   */
  public void run() {

    updateThreadName(null);

    LOG.log(Level.FINE, "NGSession {0} is waiting for first client to connect", instanceNumber);
    Socket socket = nextSocket();
    while (socket != null) {
      LOG.log(Level.FINE, "NGSession {0} accepted new connection", instanceNumber);
      try (NGCommunicator comm = communicatorCreator.get(socket)) {
        runImpl(comm, socket);
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Internal error in NGSession " + instanceNumber, t);
      }

      LOG.log(Level.FINEST, "NGSession {0} started cleanup", instanceNumber);

      if (System.in instanceof ThreadLocalInputStream) {
        ((ThreadLocalInputStream) System.in).init(null);
        ((ThreadLocalPrintStream) System.out).init(null);
        ((ThreadLocalPrintStream) System.err).init(null);
      }

      LOG.log(Level.FINE, "NGSession {0} is closing client socket", instanceNumber);
      try {
        socket.close();
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Internal error closing socket", t);
      }

      updateThreadName(null);
      sessionPool.give(this);
      socket = nextSocket();
    }

    LOG.log(Level.FINE, "NGSession {0} stopped", instanceNumber);
  }

  private void runImpl(NGCommunicator comm, Socket socket) {
    try (InputStream in = new NGInputStream(comm);
        PrintStream out = new PrintStream(new NGOutputStream(comm, NGConstants.CHUNKTYPE_STDOUT));
        PrintStream err =
            new PrintStream(new NGOutputStream(comm, NGConstants.CHUNKTYPE_STDERR)); ) {
      // ThreadLocal streams for System.in/out/err redirection
      if (System.in instanceof ThreadLocalInputStream) {
        ((ThreadLocalInputStream) System.in).init(in);
        ((ThreadLocalPrintStream) System.out).init(out);
        ((ThreadLocalPrintStream) System.err).init(err);
      }

      try {

        CommandContext cmdContext = comm.readCommandContext();

        String threadName =
            (socket.getInetAddress() == null ? "" : socket.getInetAddress().getHostAddress() + ": ")
                + cmdContext.getCommand();
        updateThreadName(threadName);

        Class cmdclass;
        try {
          Alias alias = server.getAliasManager().getAlias(cmdContext.getCommand());
          if (alias != null) {
            cmdclass = alias.getAliasedClass();
          } else if (server.allowsNailsByClassName()) {
            cmdclass = Class.forName(cmdContext.getCommand(), true, classLoader);
          } else {
            cmdclass = server.getDefaultNailClass();
          }
        } catch (ClassNotFoundException ex) {
          throw new NGNailNotFoundException("Nail class not found: " + cmdContext.getCommand(), ex);
        }

        Object[] methodArgs = new Object[1];
        Method mainMethod; // will be either main(String[]) or nailMain(NGContext)
        String[] cmdlineArgs =
            cmdContext
                .getCommandArguments()
                .toArray(new String[cmdContext.getCommandArguments().size()]);

        boolean isStaticNail = true; // See: NonStaticNail.java

        Class[] interfaces = cmdclass.getInterfaces();

        for (int i = 0; i < interfaces.length; i++) {
          if (interfaces[i].equals(NonStaticNail.class)) {
            isStaticNail = false;
            break;
          }
        }

        if (!isStaticNail) {
          mainMethod = cmdclass.getMethod("nailMain", new Class[] {String[].class});
          methodArgs[0] = cmdlineArgs;
        } else {
          try {
            mainMethod = cmdclass.getMethod("nailMain", nailMainSignature);
            NGContext context = new NGContext();
            context.setArgs(cmdlineArgs);
            context.in = in;
            context.out = out;
            context.err = err;
            context.setCommand(cmdContext.getCommand());
            context.setNGServer(server);
            context.setCommunicator(comm);
            context.setEnv(cmdContext.getEnvironmentVariables());
            context.setInetAddress(socket.getInetAddress());
            context.setPort(socket.getPort());
            context.setWorkingDirectory(cmdContext.getWorkingDirectory());
            methodArgs[0] = context;
          } catch (NoSuchMethodException toDiscard) {
            // nailMain is not found, let's try main(String[])
            try {
              mainMethod = cmdclass.getMethod("main", mainSignature);
              methodArgs[0] = cmdlineArgs;
            } catch (NoSuchMethodException ex) {
              // failed to find 'main' too, so give up and throw
              throw new NGNailNotFoundException(
                  "Can't find nailMain or main functions in " + cmdclass.getName(), ex);
            }
          }
        }

        server.nailStarted(cmdclass);

        try {
          mainMethod.invoke(isStaticNail ? null : cmdclass.newInstance(), methodArgs);
        } catch (InvocationTargetException ite) {
          throw ite.getCause();
        } finally {
          server.nailFinished(cmdclass);
        }

        // send exit code 0 to the client; if nail previously called NGSession.exit() or
        // System.exit() explicitly then this will do nothing
        comm.exit(NGConstants.EXIT_SUCCESS);

      } catch (NGExitException exitEx) {
        // We got here if nail called System.exit(). Just quit with provided exit code.
        LOG.log(Level.INFO, "Nail cleanly exited with status {0}", exitEx.getStatus());
        comm.exit(exitEx.getStatus());
      } catch (NGNailNotFoundException notFoundEx) {
        LOG.log(Level.WARNING, "Nail not found", notFoundEx);
        comm.exit(NGConstants.EXIT_NOSUCHCOMMAND);
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Nail raised unhandled exception", t);
        comm.exit(NGConstants.EXIT_EXCEPTION); // remote exception constant
      }
    } catch (IOException e) {
      // handle any exceptions thrown by close()
      LOG.log(Level.WARNING, "Nail raised an exception while closing the streams", e);
    }
  }

  /** Updates the current thread name (useful for debugging). */
  private void updateThreadName(String detail) {
    setName("NGSession " + instanceNumber + ": " + ((detail == null) ? "(idle)" : detail));
  }
}
