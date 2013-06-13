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

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * Common jobs configuration for Manager and Worker queues
 */
public class JobCommon {
	protected static final Logger log = Logger.getLogger(JobCommon.class);
	protected final int MAXJOBS;
	protected final String rootPath, newPath, activePath;
	protected final ZooKeeper zk;
	
	/**
	 * Constructor.
	 * 
	 * @param zk		Open ZooKeeper handle
	 * @param config	Nfdist's configuration
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public JobCommon(ZooKeeper zk, Configuration config) throws KeeperException, InterruptedException {
		this.zk = zk;
		this.MAXJOBS = config.getInt("jobs.max");
		
		//Check paths and create the missing ones
		rootPath = Tools.fixPath(zk, config.getString("zookeeper.path.root") + "/" + config.getString("zookeeper.path.jobs"));
		newPath = rootPath + "/" + "new";
		if (null == zk.exists(newPath, false)) {
			zk.create(newPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		activePath = rootPath + "/" + "active";
		if (null == zk.exists(activePath, false)) {
			zk.create(activePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

}
