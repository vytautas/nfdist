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

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Provides locking mechanism using ZooKeeper
 */
public class Lock {
	private ZooKeeper zk;
	private Watcher watcher;
	private String lockpath, locknode;
	private Object mutex;
	
	private static final Logger log = Logger.getLogger(Lock.class);
	private static final String pathappend = "locks";
	private static final int TIMEOUT = 5000;

	/**
	 * Event monitor
	 */
	private class LockWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			log.debug("SIGNAL:"
					+ " path="+event.getPath()
					+ " type="+event.getType()
			);
			synchronized (mutex) {
				mutex.notify();	
			}
		}
	}
	
	/**
	 * Constructor.
	 * 
	 * @param zk	Open ZooKeeper handle
	 * @param path	Nfdist's root folder on ZooKeeper
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public Lock(ZooKeeper zk, String path) throws InterruptedException, KeeperException {
		mutex = new Object();
		watcher = new LockWatcher();
		this.zk = zk;
		lockpath = Tools.fixPath(zk, path + "/" + pathappend);
		locknode = null;
	}
	
	/**
	 * Convert numerical node's full path into a number
	 * 
	 * @param node	Full node's path.
	 * @return	Node'e number.
	 */
	private long nodeToLong(String node) {
		String[] elements = node.split("/");
		return Long.parseLong(elements[elements.length-1]);
	}
	
	/**
	 * Lock queue is based on node number, the lowest number has the lock.
	 * This method returns a node with the greatest number which is still
	 * lower than ours (i.e. the lock() method should be called beforehand). 
	 * 
	 * @return Null if we are first in line, otherwise full path of the node ahead.
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private String getPrecursor() throws KeeperException, InterruptedException {
		
		final List<String> list = zk.getChildren(lockpath, false);
		if (list.isEmpty()) {
			return null;
		}
		
		final long self = nodeToLong(locknode);
		String holder = null;
		long max, num;
		
		max = 0;
		for (String node: list) {
			num = nodeToLong(node);
			if (num > max && num < self) {
				max = num;
				holder = node;
			}
		}

		if (null == holder) {
			return null;
		}
		
		return lockpath + "/" + holder;
	}
	
	/**
	 * Removes active lock if it is owned by the current session.
	 * A Lock might become stale if connection with ZooKeeper was lost.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private void breakStaleLock() throws KeeperException, InterruptedException {
		
		final List<String> list = zk.getChildren(lockpath, false);
		if (list.isEmpty()) {
			return;
		}
	
		String first = null;
		long min, num;
		
		min = Integer.MAX_VALUE;
		for (String node: list) {
				num = nodeToLong(node);
				if (num < min) {
					min = num;
					first = node;
				}
		}
		
		if (nodeToLong(locknode) == min) {
			//Current lock is active (i.e. not stale)
			return;
		}
		
		final String firstPath = lockpath + "/" + first;
		final Stat stat = zk.exists(firstPath, false);
		final long myId = zk.getSessionId();
		if (null != stat && stat.getEphemeralOwner() == myId) {
			log.debug("Removing stale lock: "+firstPath+"("+stat.getEphemeralOwner()+" == " + myId + "), current lock is "+locknode);
			zk.delete(firstPath, -1);
		}
	}

	/**
	 * Creates a lock.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void lock() throws KeeperException, InterruptedException {
		String precursor;
		
		locknode = zk.create(lockpath+"/", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);	
		synchronized (mutex) {
			while (true) {
				precursor = getPrecursor();
				if (null == precursor) {
					//we have the lock
					break;
				}
				
				try {
					breakStaleLock();
					
					//wait for a signal from the holder node
					if (zk.exists(precursor, watcher) != null) {
						log.debug("Waiting for a forerunner: " + precursor + " (local: " + locknode + ")");
						mutex.wait(TIMEOUT);	
					}
				} catch (KeeperException.ConnectionLossException e) {
					log.warn("Handled exception: "+e.getMessage());
					Thread.sleep(100);
					//retry...
				}
			}
		}
	}
	
	/**
	 * Releases the lock.
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void unlock() throws InterruptedException, KeeperException {
		try {
			zk.delete(locknode, -1);
		} catch (KeeperException.NoNodeException e) {
			//ignore
		}
		locknode = null;
	}
}
