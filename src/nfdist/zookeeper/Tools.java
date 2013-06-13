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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * Miscellaneous ZooKeeper specific tools
 */
public class Tools {
	/**
	 * Does some basic sanitation and creates any missing nodes
	 * 
	 * @param zk		Open ZooKeeper handle
	 * @param givenPath	The path to check
	 * @return Sanitized path string
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static String fixPath(ZooKeeper zk, String givenPath) throws KeeperException, InterruptedException {
		String[] nodes = givenPath.split("/");
		String path = "";
		
		for (String node: nodes) {
			// Some basic sanitation
			node = node.trim();
			if (0 == node.length() || node.equals(".") ) {
				continue;
			}
			
			path += "/" + node;
			//creates a node if it is missing
			if (null == zk.exists(path, false)) {
				zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}		
		}
		return path;
	}
	
	/**
	 * @param fullPath Full node path
	 * @return Returns node name (last path element)
	 */
	public static String nodeName(String fullPath) {
		String[] nodes = fullPath.split("/");
		int len = nodes.length;
		if (0 == len) {
			return null;
		}
 
		return (1 == len) ? "/" : nodes[len-1].trim();
	}
}
