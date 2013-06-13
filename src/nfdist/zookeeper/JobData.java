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

package nfdist.zookeeper;

import java.util.List;

import nfdist.JobProto.JobInfo;

/**
 * Job information
 */
public class JobData {
	private String path;
	private JobInfo jobInfo;
	
	/**
	 * Constructor.
	 * 
	 * @param path	Jobs node on ZooKeeper
	 * @param jobInfo	Information provided by the manager
	 */
	public JobData(String path, JobInfo jobInfo) {
		this.path = path;
		this.jobInfo = jobInfo;
	}
	
	/**
	 * @return Job id
	 */
	public String getId() {
		return Tools.nodeName(path);
	}
	
	/**
	 * @return	Netflow file
	 */
	public String getNfFile() {
		return jobInfo.getNfFile();
	}
	
	/**
	 * @return	Nfdump filter
	 */
	public String getFilter() {
		return jobInfo.getFilter();
	}
	
	/**
	 * @return	Nfdump arguments
	 */
	public List<String> getArgs() {
		return jobInfo.getArgsList();
	}
}
