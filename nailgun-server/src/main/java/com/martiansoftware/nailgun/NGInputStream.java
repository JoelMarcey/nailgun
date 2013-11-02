/*

  Copyright 2004-2012, Martian Software, Inc.

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

package com.martiansoftware.nailgun;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

/**
 * A FilterInputStream that is able to read the chunked stdin stream
 * from a NailGun client.
 *
 * @author <a href="http://www.martiansoftware.com/contact.html">Marty Lamb</a>
 */
public class NGInputStream extends FilterInputStream implements Closeable {

    // An NGInputStream is required per NGSession and each NGInputStream requires one thread to loop reading chunks
    // by executing Runnables on another thread with a timeout. So, the thread pool size needs to be twice the
    // DEFAULT_SESSIONPOOL size.
    private static final ExecutorService executor = Executors.newFixedThreadPool(NGServer.DEFAULT_SESSIONPOOLSIZE * 2);
    private final DataInputStream din;
    private InputStream stdin = null;
    private boolean eof = false;
    private long remaining = 0;
    private byte[] oneByteBuffer = null;
    private final DataOutputStream out;
    private boolean started = false;
    private long lastReadTime = System.currentTimeMillis();
    private final Future readFuture;
    private final Set clientListeners = new HashSet();

    /**
     * Creates a new NGInputStream wrapping the specified InputStream.
     * Also sets up a timer to periodically consume heartbeats sent from the client and
     * call registered NGClientListeners if a client disconnection is detected.
     * @param in the InputStream to wrap
     * @param out the OutputStream to which SENDINPUT chunks should be sent
     * @param serverLog the PrintStream to which server logging messages should be written
     */
    public NGInputStream(InputStream in, DataOutputStream out, final PrintStream serverLog) {
        super(in);
        din = (DataInputStream) this.in;
        this.out = out;

        final Thread mainThread = Thread.currentThread();
        readFuture = executor.submit(new Runnable(){
            public void run() {
                try {
                    Thread.currentThread().setName(mainThread.getName() + " read stream thread (NGInputStream pool)");
                    while(true) {
                        Future readHeaderFuture = executor.submit(new Runnable(){
                            public void run() {
                                Thread.currentThread().setName(mainThread.getName() + " read chunk thread (NGInputStream pool)");
                                try {
                                    readChunk();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    Thread.currentThread().setName(Thread.currentThread().getName() + " (idle)");
                                }
                            }
                        });
                        try {
                            readHeaderFuture.get(NGConstants.HEARTBEAT_INTERVAL_MILLIS * 10, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                            if (! isClientConnected()) {
                                break;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                } catch (ExecutionException e) {
                } finally {
                    readEof();
                    notifyClientListeners(serverLog, mainThread);
                    Thread.currentThread().setName(Thread.currentThread().getName() + " (idle)");
                }
            }
        });
    }

    /**
     * Calls clientDisconnected method on all registered NGClientListeners.
     * If any of the clientDisconnected methods throw an NGExitException due to calling System.exit()
     * clientDisconnected processing is halted, the exit status is printed to the serverLog and the main
     * thread is interrupted.
     * @param serverLog The NailGun server log stream.
     * @param mainThread The thread running nailMain which should be interrupted on System.exit()
     */
    private synchronized void notifyClientListeners(PrintStream serverLog, Thread mainThread) {
        try {
            for (Iterator i = clientListeners.iterator(); i.hasNext();) {
                ((NGClientListener) i.next()).clientDisconnected();
            }
            serverLog.println(mainThread.getName() + " disconnected");
        }
        catch (NGExitException e) {
            serverLog.println(mainThread.getName() + " exited with status " + e.getStatus());
            mainThread.interrupt();
        }
        finally {
            clientListeners.clear();
        }
    }

    /**
     * Cancels the thread reading from the NailGun client.
     */
    public synchronized void close() {
        readFuture.cancel(true);
    }

    /**
     * Reads a NailGun chunk payload from {@link #in} and returns an InputStream that reads from
     * that chunk.
     * @param in the InputStream to read the chunk payload from.
     * @param len the size of the payload chunk read from the chunkHeader.
     * @return an InputStream containing the read data.
     * @throws IOException if thrown by the underlying InputStream,
     * or if the stream EOF is reached before the payload has been read.
     */
    private InputStream readPayload(InputStream in, int len) throws IOException {

        byte[] receiveBuffer = new byte[len];
        int totalRead = 0;
        while (totalRead < len) {
            int currentRead = in.read(receiveBuffer, totalRead, len - totalRead);
            if (currentRead < 0) {
                throw new IOException("stdin EOF before payload read.");
            }
            totalRead += currentRead;
        }
        return new ByteArrayInputStream(receiveBuffer);
    }

    /**
     * Reads a NailGun chunk header from the underlying InputStream.
     *
     * @throws IOException if thrown by the underlying InputStream,
     * or if an unexpected NailGun chunk type is encountered.
     */
    private synchronized void readChunk() throws IOException {

        int hlen = din.readInt();
        byte chunkType = din.readByte();
        lastReadTime = System.currentTimeMillis();
        switch(chunkType) {
            case NGConstants.CHUNKTYPE_STDIN:
                if (remaining != 0) throw new IOException("Data received before stdin stream was emptied.");
                remaining = hlen;
                stdin = readPayload(in, hlen);
                notify();
                break;

            case NGConstants.CHUNKTYPE_STDIN_EOF:
                readEof();
                break;

            case NGConstants.CHUNKTYPE_HEARTBEAT:
                break;

            default:
                throw(new IOException("Unknown stream type: " + (char) chunkType));
        }
    }

    /**
     * Notify threads waiting in waitForChunk on either EOF chunk read or client disconnection.
     */
    private synchronized void readEof() {
        eof = true;
        notifyAll();
    }

    /**
     * @see java.io.InputStream#available()
     */
    public int available() throws IOException {
        if (eof) return(0);
        if (stdin == null) return(0);
        return stdin.available();
    }

    /**
     * @see java.io.InputStream#markSupported()
     */
    public boolean markSupported() {
        return (false);
    }

    /**
     * @see java.io.InputStream#read()
     */
    public synchronized int read() throws IOException {
        if (oneByteBuffer == null) oneByteBuffer = new byte[1];
        return((read(oneByteBuffer, 0, 1) == -1) ? -1 : (int) oneByteBuffer[0]);
    }

    /**
     * @see java.io.InputStream.read(byte[])
     */
    public int read(byte[] b) throws IOException {
        return (read(b, 0, b.length));
    }

    /**
     * @see java.io.InputStream.read(byte[],offset,length)
     */
    public synchronized int read(byte[] b, int offset, int length) throws IOException {
        if (!started) {
            sendSendInput();
        }

        waitForChunk();
        if (eof) return(-1);

        int bytesToRead = Math.min((int) remaining, length);
        int result = stdin.read(b, offset, bytesToRead);
        remaining -= result;
        if (remaining == 0) sendSendInput();
        return (result);
    }

    /**
     * If EOF chunk has not been received, but no data is available, block until data is received, EOF or disconnection.
     * @throws IOException which just wraps InterruptedExceptions thrown by wait.
     */
    private synchronized void waitForChunk() throws IOException {
        try {
            if((! eof) && (remaining == 0)) wait();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private synchronized void sendSendInput() throws IOException {
        out.writeInt(0);
        out.writeByte(NGConstants.CHUNKTYPE_SENDINPUT);
        out.flush();
        started = true;
    }

    /**
     * @return true if interval since last read is less than 10 times expected heartbeat interval.
     */
    public boolean isClientConnected() {
        long intervalMillis = System.currentTimeMillis() - lastReadTime;
        return intervalMillis < (NGConstants.HEARTBEAT_INTERVAL_MILLIS * 10);
    }

    /**
     * @param listener the {@link NGClientListener} to be notified of client events.
     */
    public synchronized void addClientListener(NGClientListener listener) {
    if (readFuture.isDone()) {
        listener.clientDisconnected(); // Client has already disconnected, so call clientDisconnected immediately.
    } else {
        clientListeners.add(listener);
    }
    }

    /**
     * @param listener the {@link NGClientListener} to no longer be notified of client events.
     */
    public synchronized void removeClientListener(NGClientListener listener) {
        clientListeners.remove(listener);
    }
}
