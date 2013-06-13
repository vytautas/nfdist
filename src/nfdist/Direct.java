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

import nfdist.hadoop.FileList;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/**
 * Provides methods to execute nfdump directly.
 */
public class Direct extends Proc {
	private final int BUFSIZE;
	private final String NFDUMP;
	private final Thread procIn, procOut, procErr;
	private final FileSystem fs;
	private List<FileStatus> stats;
	private Process proc;
	
	/**
	 * Reads netflow files stored in HDFS and writes the data into nfdump's stdin.
	 */
	private class ProcIn implements Runnable {
		@Override
		public void run() {
			final WritableByteChannel inChannel = Channels.newChannel(proc.getOutputStream());
			final ByteBuffer dataBB = ByteBuffer.allocate(BUFSIZE);
			
			FSDataInputStream hdfsIn = null;
			
			try {
				for (FileStatus s: stats) {
					hdfsIn = fs.open(s.getPath());
					while (hdfsIn.read(dataBB) > 0) {
						dataBB.flip();
						inChannel.write(dataBB);
						dataBB.clear();
					}
					hdfsIn.close();
				}
				inChannel.close();
			} catch (IOException e) {
				log.debug("Jobs interrupted by: "+e.getMessage());
				
				//Cleanup
				try {
					inChannel.close();
				} catch (IOException e1) {
					//ignore
				}
			}
		}
	}
	
	/**
	 * Proxy nfdumps's stdout.
	 */
	private class ProcOut implements Runnable {
		@Override
		public void run() {		
			StreamToStd(proc.getInputStream(), System.out, BUFSIZE);
		}
	}
	
	/**
	 * Proxy nfdumps's stderr.
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
	 * @param fs			Open HDFS handle
	 * @param files			List of files to read
	 * @param config		Nfdist's configuration
	 * @throws IOException
	 */
	public Direct(FileSystem fs, FileList files, Configuration config) throws IOException {
		super(3); //Three sub-threads (std-in/out/err)
			
		this.BUFSIZE = config.getInt("jobs.combiner.bufsize");
		
		if (files.oneFile()) {
			// Only one file, use nfdump directly
			this.NFDUMP = config.getString("local.path.nfdump");
		} else {
			// Use the combiner if more than one file is provided
			this.NFDUMP = config.getString("local.path.nfcat");
		}
		
		this.procIn = new Thread(new ProcIn());
		this.procOut = new Thread(new ProcOut());
		this.procErr = new Thread(new ProcErr());
		
		this.fs = fs;
		this.stats = files.getStats();
	}
	
	/**
	 * Start the Direct processing 
	 * 
	 * @param args		Nfdump arguments
	 * @param filter	Netflow filter
	 * @throws IOException
	 */
	public void start(List<String> args, String filter) throws IOException  {
		List<String> cmd = new ArrayList<String>();
		cmd.add(NFDUMP);
		cmd.addAll(args);		
		cmd.add(filter);
		
		log.info("Starting direct combiner: "+StringUtils.join(cmd, ' '));
		proc = new ProcessBuilder(cmd).start();
		
		activate(procIn);
		activate(procOut);
		activate(procErr);
	}
}
