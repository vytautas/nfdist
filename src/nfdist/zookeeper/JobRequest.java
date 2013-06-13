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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.configuration.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Managers job queue
 */
public class JobRequest extends JobCommon {
	
	private final HashMap<String, Integer> active = new HashMap<String, Integer>();
	private final ArrayBlockingQueue<String> completed;
	private boolean canceled=false;
	private Integer activeCount=0;
	private int completedCount=0;
	
	/**
	 * Reduce number of active jobs
	 */
	private synchronized void countDown() {
		activeCount--;
		synchronized (activeCount) {
			activeCount.notify();
		}
		
		//check if all jobs completed
		if (0 == activeCount) {
			this.notify();
		}
	}
	
	/**
	 * @return Ids of registered jobs
	 */
	private synchronized List<String> getRegistered() {
		return new ArrayList<String>(active.keySet());
	}
	
	/**
	 * Registers a job 
	 * @param name	ZooKeeper node name
	 */
	private synchronized void registerJob(String name) {
		active.put(Tools.nodeName(name), 1);
		activeCount++;
	}

	/**
	 * Removes finished job from the register of active jobs 
	 * @param path	ZooKeeper node name
	 */
	private synchronized void removeJob(String path) {
		active.remove(Tools.nodeName(path));
		countDown();
	}

	/**
	 * Check if job is finished and continue watching the node
	 * 
	 * @param path	ZooKeeper node name
	 * @return	True if job is completed
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	//
	private boolean isFinished(String path) throws InterruptedException, KeeperException {
		final byte[] bytes = zk.getData(path, true, null);
		if (null != bytes) {
			String data = new String(bytes);
			if (data.equals("done")) {
				return true;
			}
			else if (data.equals("failed")) {
				//TODO: better handle failures
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Limit number of maximum active jobs. If the limit is reached, blocks until an active job finishes.
	 */
	private void checkLimit() {
		synchronized (activeCount) {
			while (activeCount >= MAXJOBS) {
				try {
					activeCount.wait(100);
				} catch (InterruptedException e) {
					//ignore
				}
			}
		}
	}
	
	/**
	 * Constructor.
	 * 
	 * @param zk		ZooKeeper's handle
	 * @param config	Nfdist configuration
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public JobRequest(ZooKeeper zk, Configuration config) throws KeeperException, InterruptedException {
		super(zk, config);
		completed = new ArrayBlockingQueue<String>(MAXJOBS);
	}

	/**
	 * Creates a new job
	 * 
	 * @param data	Serialized job parameters
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void create(byte[] data) throws KeeperException, InterruptedException {
		//limit active jobs
		checkLimit();
		
		final String name = zk.create(newPath+"/", data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		final String id = Tools.nodeName(name);
		registerJob(name);
		log.debug(id + " job created.");
		
		//Watch for job activation
		String activeName = activePath+"/"+id;
		if (zk.exists(activeName, true) != null) {
			log.debug("Job already active:"+activeName);
			completeIfFinished(activeName);
		}
	}

	/**
	 * @param path	ZooKeeper's node name
	 * @return	True if job was registered by this Manager
	 */
	public boolean isRegistered(String path) {
			return (active.get(Tools.nodeName(path)) > 0 ? true : false);
	}

	/**
	 * @return	True if all completed jobs are combined and there are no more active ones.
	 */
	public synchronized boolean allDone() {
		return completed.isEmpty() && (0 == activeCount);
	}
	
	/**
	 * @return	Number of completed jobs waiting for combiner.
	 */
	public int getCompletedCount() {
		return completedCount;
	}

	/**
	 * Takes the completed job. It will be considered as handled by the Combiner.
	 * 
	 * @return Completed job id
	 * @throws InterruptedException
	 */
	public String takeCompleted() throws InterruptedException {
		return completed.take(); 
	}

	/**
	 * Mark job as completed If a Filter finished. 
	 * @param path	Path to a ZooKeeper's node
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void completeIfFinished(String path) throws InterruptedException, KeeperException {
		// isCompleted also (re)creates the watch
		if (isFinished(path)) {
			log.debug("Job finished: "+path);
			zk.delete(path, -1);
			removeJob(path);
			completed.put(Tools.nodeName(path));
			completedCount++;
		}
	}
	
	/**
	 * Cancel all waiting jobs.
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void cancelAll() throws KeeperException, InterruptedException {
		canceled = true;
		final Lock l = new Lock(zk, rootPath);
		
		l.lock();
		final List<String> ids = getRegistered();
		log.debug("Canceling remaining job requests.");
		for (String id: ids) {
			try {
				//try removing the job request
				zk.delete(newPath+"/"+id, -1);
				//also remove from the job map
				removeJob(id);
				log.debug(id + " job canceled.");
			} catch (KeeperException.NoNodeException e) {
				//job already started, ignore
				continue;
			} catch (KeeperException e) {
				l.unlock();
				throw e;
			} catch (InterruptedException e) {
				l.unlock();
				throw e;
			}
		}
		l.unlock();
	}
	
	/**
	 * @return	True if processing was canceled
	 */
	public boolean isCanceled() {
		return canceled;
	}
	
	//Wait for active jobs to finish
	/**
	 * @param timeout	Time limit for completing all jobs
	 * @return	True if all jobs were completed in time.
	 */
	public synchronized boolean await(long timeout) {
		while (activeCount>0) {
			try {
				wait(timeout);
				if (activeCount>0) {
					//Timeout
					return false;
				}
			} catch (InterruptedException e) {
				//Ignore
			}
		}
		return true;
	}
}
