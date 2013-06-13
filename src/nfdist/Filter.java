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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import nfdist.hadoop.TmpPath;
import nfdist.zookeeper.JobData;

/**
 * Called by the Worker to process individual job requests. The results
 * are stored in a temporary directory and should be moved into HDFS
 * where the Combiner can access them.
 */
public class Filter extends Proc  {
	private final String NFDUMP;
	private final int BUFSIZE;

	private final Thread procIn, procOut, procErr;
	private final JobData jobData;
	private final FileSystem fs;
	private final String id;
	private Process proc;
	private TmpPath localTmp;
	private boolean success;
	
	/**
	 * Reads netflow files stored in HDFS and writes the data into nfdump's stdin.
	 */
	private class ProcIn implements Runnable {

		@Override
		public void run() {
			final String id = jobData.getId();
			final Path inFilePath = new Path(fs.getUri().toString()+jobData.getNfFile());
			final ByteBuffer dataBB = ByteBuffer.allocate(BUFSIZE);
			
			try {
				final WritableByteChannel inChannel = Channels.newChannel(proc.getOutputStream());
				final FSDataInputStream hdfsIn = fs.open(inFilePath);
				
				try {
					log.debug(id + " data piping started");
					while (hdfsIn.read(dataBB) > 0) {
						dataBB.flip();
						inChannel.write(dataBB);
						dataBB.clear();
					}
					log.debug(id + " data piping completed.");
					success=true;
				} catch (IOException e) {
					log.debug(id + " data piping interrupted by exception ("+ e.getMessage() + ").");
					//most likely it will be a broken pipe, ignore it...
					success=true;
				}
				inChannel.close();
				hdfsIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Proxy nfdumps's stdout.
	 */
	private class ProcOut implements Runnable {
		@Override
		public void run() {		
			StreamToLog(proc.getInputStream(), BUFSIZE, id+" STDOUT");
		}
	}
	
	/**
	 * Proxy nfdumps's stderr.
	 */
	private class ProcErr implements Runnable {
		@Override
		public void run() {
			StreamToLog(proc.getErrorStream(), BUFSIZE, id+" STDERR");
		}
	}
	
	/**
	 * Constructor
	 * 
	 * @param jobData	Data about a specific job
	 * @param fs		Open HDFS handle
	 * @param config	Nfdist configuration
	 */
	public Filter(JobData jobData, FileSystem fs, Configuration config) {
		super(3); //Three sub-threads
		
		this.BUFSIZE = config.getInt("jobs.filter.bufsize");
		this.NFDUMP = config.getString("local.path.nfdump");
		this.localTmp = new TmpPath(config.getString("local.path.tmp"));
		
		this.procIn = new Thread(new ProcIn());
		this.procOut = new Thread(new ProcOut());
		this.procErr = new Thread(new ProcErr());
		
		this.jobData = jobData;
		this.fs = fs;
		this.success = false;
		
		this.id = jobData.getId();
	}
	
	/**
	 * Start the Filter.
	 * 
	 * @throws IOException
	 */
	public void start() throws IOException {
		final String filter = jobData.getFilter();
		final List<String> args = jobData.getArgs();
		final List<String> cmd = new ArrayList<String>();
		
		cmd.add(NFDUMP);
		cmd.add("-z");
		cmd.add("-w");
		cmd.add(localTmp.asString(id));
		cmd.addAll(args);
		cmd.add(filter);
		log.info(id + " filtering netflow file: " + jobData.getNfFile()
				  + " args: '" + StringUtils.join(args, ' ') + "' filter: '" + filter + "'.");
		proc = new ProcessBuilder(cmd).start();
		
		activate(procIn);
		activate(procOut);
		activate(procErr);
	}
	
	/**
	 * Status of the processing.
	 * 
	 * @return	True if successful.
	 */
	public boolean success() {
		return success;
	}


}
