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

import java.io.IOException;
import java.util.List;

import nfdist.JobProto.JobInfo;

import org.apache.commons.configuration.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Worker's job queue
 */
public class JobQueue extends JobCommon {
	private final Object mutex;
	private final JobWatcher watcher;
	private final String myName;
	
	private static final int TIMEOUT = 10000;
	
	/**
	 * Notifies when new jobs become available
	 */
	private class JobWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			synchronized (mutex) {
				//log.debug("SIGNAL:"
				//+ " path="+event.getPath()
				//+ " type="+event.getType()
				//	);
				mutex.notify();
			}
		}
	}

	/**
	 * Constructor.
	 * 
	 * @param zk		Open ZooKeeper handle
	 * @param config	Nfdist's configuration
	 * @param name		Worker node name. Must be the same as datanode name. 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public JobQueue(ZooKeeper zk, Configuration config, String name) throws KeeperException, InterruptedException {
		super(zk, config);
		this.myName = name;
		this.mutex = new Object();
		this.watcher = new JobWatcher();
	}
	
	/**
	 * Gets available job or waits until one becomes available.
	 * 
	 * @return	JobData (id, file, nfdump arguments, filter)
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws IOException
	 */
	public JobData take() throws InterruptedException, KeeperException, IOException {
		Lock l = new Lock(zk, rootPath);
		JobData jobData;
		
		synchronized (mutex) {
			while (true) {
				log.debug("Searching for a job...");
				l.lock();
				
				jobData = get();
				if (null != jobData) {
					activate(jobData.getId());
					log.debug("Unlocking and finishing take()");
					l.unlock();
					return jobData;
				} else {
					log.debug("Unlocking and waiting...");
					l.unlock();
					mutex.wait(TIMEOUT);
				}
			}
		}
	}

	/**
	 * Try to get job information.
	 * 
	 * @return	JobData if job is available or null.
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private JobData get() throws IOException, KeeperException, InterruptedException {
		byte[] data;
		String jobPath;
		JobInfo jobInfo;
		List<String> servers;

		final List<String> list = zk.getChildren(newPath, watcher);

		for (String job: list) {
			jobPath = newPath+"/"+job;
			
			try {
				data = zk.getData(jobPath, false, null);
			} catch (KeeperException.NoNodeException e) {
				//Job got canceled?
				return null;
			}
			
			if (null != data) {
				jobInfo = JobInfo.parseFrom(data);
				servers = jobInfo.getServerList();
				for (String s: servers) {
					if (myName.equals(s)) {
						log.debug("Found a job: "+job);
						return new JobData(jobPath, jobInfo);
					} else {
						log.debug("No local data, skipping job: "+job);
					}
				}
			}
		}
		log.debug("No jobs available at the moment.");
		return null;
	}

	/**
	 * Mark job as active
	 * 
	 * @param id	Job id
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	private void activate(String id) throws InterruptedException, KeeperException {
		log.debug("Activating job: "+id);
		
		//remove job request node
		try {
			zk.delete(newPath+"/"+id, -1);
		} catch (KeeperException.NoNodeException e) {
			// Parent gone with the job? Ignore.
		}
		
		//create active job node
		zk.create(activePath+"/"+id, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	/**
	 * Mark job as finished.
	 * 
	 * @param id	Job id
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void finish(String id) throws KeeperException, InterruptedException {
		zk.setData(activePath+"/"+id, "done".getBytes(), 0);
	}

	/**
	 * Mark job as failed.
	 * 
	 * @param id	Job id
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void failed(String id) throws KeeperException, InterruptedException {
		zk.setData(activePath+"/"+id, "failed".getBytes(), 0);
	}
	
}
