/*
 *  Copyright (c) 2013, Vytautas Krakauskas
 *  Copyright (c) 2013, Kaunas university of technology
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 *  TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package nfdist;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

/**
 * A generic pipe class to handle std-in/out/err streams.
 */
public class Proc {
	protected static Logger log = Logger.getLogger(Proc.class);
	private final ArrayBlockingQueue<Thread> threads;
	
	/**
	 * Generic method for writing a stream to a log file.
	 * 
	 * @param stream	Stream handle
	 * @param bufsize	Buffer size in bytes
	 * @param prefix	Prepend each output with this string.
	 */
	private void ToLog(InputStream stream, int bufsize, String prefix) {
		final ReadableByteChannel channel = Channels.newChannel(stream);
		final ByteBuffer buffer = ByteBuffer.allocate(bufsize);
		
		int read;
		try {
			while ((read = channel.read(buffer)) > 0 && channel.isOpen()) {
				buffer.flip();
				log.info(prefix + ": " + new String(buffer.array(), 0, read));
				buffer.clear();
			}
			channel.close();
		} catch (AsynchronousCloseException e) {
			log.debug("Channel was asynchronously closed...");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write a stream to a log file.
	 * @param stream	Stream handle
	 * @param bufsize	Buffer size in bytes
	 */
	protected void StreamToLog(InputStream stream, int bufsize) {
		ToLog(stream, bufsize, "");
	}
	
	/**
	 * Write a stream to a log file with prefix string.
	 * @param stream	Stream handle
	 * @param bufsize	Buffer size in bytes
	 * @param prefix	Prepend each output with this string.
	 * @see ToLog()
	 */
	protected void StreamToLog(InputStream stream, int bufsize, String prefix) {
		ToLog(stream, bufsize, prefix);
	}
	
	/**
	 * Write a stream to a standard output/error stream
	 * @param stream	Stream handle
	 * @param out		Output or error stream handle
	 * @param bufsize	Buffer size in bytes
	 */
	protected void StreamToStd(InputStream stream, PrintStream out, int bufsize) {
		final ReadableByteChannel channel = Channels.newChannel(stream);
		final ByteBuffer buffer = ByteBuffer.allocate(bufsize);
		
		int read;
		try {
			while ((read = channel.read(buffer)) > 0 && channel.isOpen()) {
				buffer.flip();
				out.print(new String(buffer.array(), 0, read));
				buffer.clear();
			}
			channel.close();
		} catch (AsynchronousCloseException e) {
			log.debug("Channel was asynchronously closed...");
		} catch (IOException e) {
			//e.printStackTrace();
		}
	}
	
	/**
	 * Start a thread and add it to a pool.
	 * @param thread	Thread handle
	 */
	protected void activate(Thread thread) {
		thread.start();
		threads.add(thread);
	}
	
	/**
	 * Constructor
	 * @param num	Thread pool size.
	 */
	public Proc(int num) {
		this.threads = new ArrayBlockingQueue<Thread>(num);
	}

	/**
	 * Wait for threads to complete or terminate them after a timeout
	 * @param timeout	Amount of time in milliseconds before terminating threads.
	 * @return	True if threads completed on their own.
	 */
	public boolean await(long timeout) {
		long deadline = System.currentTimeMillis() + timeout;
		long wait;
		boolean result=true;
		Thread thread;
				
		while (!threads.isEmpty()) {
	        try {
	        	thread = threads.remove();
	        	log.debug(thread.getName() + " waiting.");
	        	while (thread.isAlive()) {
	        		try {
		        		if (System.currentTimeMillis() >= deadline) {
		        			log.debug("Thread "+thread.getName()+" timeout, terminating...");
		        			thread.interrupt();
		        			//Small chance to exit cleanly
		        			thread.join(1000);
		        			result = false;
		        		} else {
		        			wait = deadline - System.currentTimeMillis();
		        			if (wait > 0) {
		        				thread.join(wait);
		        			}
		        		}
	        		}
	        		catch (InterruptedException e) {
	        			e.printStackTrace();	        			
	        		}
	        	}
	        	log.debug(thread.getName() + " finished.");
			}
	        catch (NoSuchElementException e) {
	        	e.printStackTrace();
			}
	    }
		return result;
	}

}
