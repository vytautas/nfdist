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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import nfdist.hadoop.TmpPath;
import nfdist.zookeeper.JobRequest;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

/**
 * Provides methods for combiner control.
 */
public class Combiner extends Proc {
	private final int BUFSIZE;
	private final String NFCAT;
	
	private final Thread procIn, procOut, procErr;
	private final FileSystem fs;
	private final JobRequest jobs;
	private Process proc;
	private TmpPath hdfsTmp;
	
	/**
	 * Waits for job results, reads them from HDFS and writes into nfcat's stdin.
	 */
	private class ProcIn implements Runnable {
		@Override
		public void run() {
			//final WritableByteChannel inChannel = Channels.newChannel(proc.getOutputStream());
			final OutputStream output = proc.getOutputStream();
			final ByteBuffer dataBB = ByteBuffer.allocate(BUFSIZE);
			
			String id;
			Path inFilePath;
			FSDataInputStream hdfsIn = null;
			
			try {
				while (!jobs.allDone() || jobs.getCompletedCount() == 0) {
					id = jobs.takeCompleted();
					inFilePath = hdfsTmp.asPath(id);
					log.debug(id + " reading results.");
					try {
						hdfsIn = fs.open(inFilePath);
						while (hdfsIn.read(dataBB) > 0) {
							dataBB.flip();
							
							/* * * * * * * * * * * * * * * * * * * * *   
							ByteChannels do not notify when the stream is closed.
							Copy the data to a byte array and write it directly to the stream.
							 * * * * * * * * * * * * * * * * * * * * */
							//inChannel.write(dataBB);
							byte[] buff = new byte[dataBB.remaining()];
							dataBB.get(buff);
							output.write(buff);
							output.flush();
							
							dataBB.clear();
						}
					}
					catch (FileNotFoundException e) {
						log.debug(id+" job has no data.");
						continue;
					}
					catch (IOException e) {
						//Process has quit on us, do the cleanup and exit.
						//Broken pipe is normal when nfdump limits its output (-c option).
						log.debug("Finishing early due to exception: "+e.getMessage());
						
						//close the current file
						hdfsIn.close();
						fs.delete(inFilePath, false);
						
						log.info("Early result, canceling remaining jobs.");
						jobs.cancelAll();
						
						//remove remaining files
						while (!jobs.allDone()) {
							try {
								id = jobs.takeCompleted();
								inFilePath = hdfsTmp.asPath(id);
								log.debug("Cleanup due to exception, removing: "+inFilePath.getName());
								fs.delete(inFilePath, false);
							}
							catch (InterruptedException e2) {
								log.warn("Interupted while cleaning: "+inFilePath.getName());
								break;
							}
						}
						//inChannel.close();
						output.close();
						log.debug("Cleanup finished successfully.");
						return;
					}
					log.debug("Closing & Removing: "+inFilePath.getName());
					hdfsIn.close();
					fs.delete(inFilePath, false);
				}
				//inChannel.close();
				output.close();
			} catch (InterruptedException e) {
				log.info("Interrupted, quiting...");
				// this will force other channels to terminate
				proc.destroy();
			} catch (IOException e) {
				log.error("Unhandled exception: "+e.getMessage());
				e.printStackTrace();
			} catch (KeeperException e) {
				log.error("Unhandled exception: "+e.getMessage());
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * Proxy nfcat's stdout.
	 */
	private class ProcOut implements Runnable {
		@Override
		public void run() {
			StreamToStd(proc.getInputStream(), System.out, BUFSIZE);
		}
	}

	/**
	 * Proxy nfcat's stderr.
	 */
	private class ProcErr implements Runnable {
		@Override
		public void run() {
			StreamToStd(proc.getErrorStream(), System.err, BUFSIZE);
		}
	}
	
	/**
	 * Class constructor
	 * 
	 * @param fs		Open HDFS handle
	 * @param jobs		Job queue
	 * @param config 	Nfdist's configuration parameters
	 */
	public Combiner(FileSystem fs, JobRequest jobs, Configuration config) {
		super(3); //Three sub-threads (std-in/out/err)
		
		this.BUFSIZE = config.getInt("jobs.combiner.bufsize");
		this.NFCAT = config.getString("local.path.nfcat");
		this.hdfsTmp = new TmpPath(config.getString("hdfs.path.tmp"));
		
		this.procIn = new Thread(new ProcIn());
		this.procOut = new Thread(new ProcOut());
		this.procErr = new Thread(new ProcErr());
		
		this.fs = fs;
		this.jobs = jobs;
	}

	/**
	 * Starts the combiner and feeds it results as they come.
	 * 
	 * @param args		Nfdump arguments
	 * @param filter	Filter string
	 * @throws IOException
	 */
	public void start(List<String> args, String filter) throws IOException {
		List<String> cmd = new ArrayList<String>();
		cmd.add(NFCAT);
		cmd.addAll(args);
		
 		//Shadow profiles use ident filters, they might not work when aggregating and combining
		String andor = "(?:\\s+(?:and|or)\\s+)?";
		filter = filter.replaceAll(andor+"ident\\s+\\w+"+andor, " ");
		filter = filter.replaceAll(andor+"\\(\\s*\\)"+andor, " ");
		
		cmd.add(filter);
		
		log.info("Starting combiner: "+StringUtils.join(cmd, ' '));
		proc = new ProcessBuilder(cmd).start();
		
		activate(procIn);
		activate(procOut);
		activate(procErr);
	}
	
	/**
	 * A method to stop combiner in case of error.
	 */
	public void stop() {
		procIn.interrupt();
	}

}
