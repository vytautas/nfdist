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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

/**
 * Class for nfdump calls where no files need to be processed.
 */
public class NoFiles extends Proc {
	private final int BUFSIZE;
	private final String NFDUMP;
	private final Thread procOut, procErr;
	private Process proc;
	
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
	 * Constructor.
	 * 
	 * @param config	Nfdist's configuration
	 * @throws IOException
	 */
	public NoFiles(Configuration config) throws IOException {
		super(2); //Two sub-threads
		
		this.BUFSIZE = config.getInt("jobs.combiner.bufsize");
		this.NFDUMP = config.getString("local.path.nfdump");
		
		this.procOut = new Thread(new ProcOut());
		this.procErr = new Thread(new ProcErr());
	}
	
	/**
	 * Start nfdump without file processing.
	 * 
	 * @param args		Nfdump arguments
	 * @param filter	Netflow filter string
	 * @throws IOException
	 */
	public void start(List<String> args, String filter) throws IOException  {
		List<String> cmd = new ArrayList<String>();
		cmd.add(NFDUMP);
		cmd.addAll(args);
		cmd.add(filter);
		log.info("Using nfdump passthrough: "+StringUtils.join(cmd, ' '));
		proc = new ProcessBuilder(cmd).start();
		
		activate(procOut);
		activate(procErr);
		
	}
}
