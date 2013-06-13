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

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper class which automatically reconnects and retries if a
 * connection was lost.
 */
public class ReconnectingZooKeeper extends ZooKeeper {
	private static final Logger log = Logger.getLogger(Lock.class);

	/**
	 * Constructor.
	 * 
	 * @param config	Nfdist configuration
	 * @param watcher	ZooKeeper event watcher
	 * @throws IOException
	 */
	public ReconnectingZooKeeper(Configuration config, Watcher watcher) throws IOException {
		super(config.getString("zookeeper.server"), config.getInt("zookeeper.timeout"), watcher);
	}
	
	@Override
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
		while (true) {
			try {
				return super.create(path, data, acl, createMode);
			} catch (KeeperException.ConnectionLossException e) {
				log.debug("Connection lost, retrying...");
				Thread.sleep(100);
			}
		}
	}

	@Override
	public void delete(String path, int version) throws InterruptedException, KeeperException {
		while (true) {
			try {
				super.delete(path, version);
				break;
			} catch (KeeperException.ConnectionLossException e) {
				log.debug("Connection lost, retrying...");
				Thread.sleep(100);
			}
		}
	}

	@Override
	public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
		while (true) {
			try {
				return super.exists(path, watch);
			} catch (KeeperException.ConnectionLossException e) {
				log.debug("Connection lost, retrying...");
				Thread.sleep(100);
			}
		}
	}
	
	@Override
	public Stat exists(String path, Watcher watcher) throws KeeperException,
			InterruptedException {
		while (true) {
			try {
				return super.exists(path, watcher);
			} catch (KeeperException.ConnectionLossException e) {
				log.debug("Connection lost, retrying...");
				Thread.sleep(100);
			}
		}
	}
	
	@Override
	public List<String> getChildren(String path, boolean watch)	throws KeeperException, InterruptedException {
		while (true) {
			try {
				return super.getChildren(path, watch);
			} catch (KeeperException.ConnectionLossException e) {
				log.debug("Connection lost, retrying...");
				Thread.sleep(100);
			}
		}
	}
	
	@Override
	public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
		while (true) {
			try {
				return super.getChildren(path, watcher);
			} catch (KeeperException.ConnectionLossException e) {
				log.debug("Connection lost, retrying...");
				Thread.sleep(100);
			}
		}
	}
	
	@Override
	public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
		while (true) {
			try {
				return super.getData(path, watch, stat);
			} catch (KeeperException.ConnectionLossException e) {
				log.debug("Connection lost, retrying...");
				Thread.sleep(100);
			}
		}
	}

	@Override
	public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
		while (true) {
			try {
				return super.setData(path, data, version);
			} catch (KeeperException.ConnectionLossException e) {
				log.debug("Connection lost, retrying...");
				Thread.sleep(100);
			}
		}
	}
}
