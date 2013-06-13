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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import nfdist.hadoop.TmpPath;
import nfdist.zookeeper.JobData;
import nfdist.zookeeper.JobQueue;
import nfdist.zookeeper.ReconnectingZooKeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Class used for distributed processing on each node.
 * 
 * Worker uses Filters to processes job requests from Managers and
 * moves the intermediate results from local file system into HDFS
 * where the Combiner can access them.
 */
public class Worker {
	private static final Logger log = Logger.getLogger(Worker.class);
	
	private static Configuration config;
	private static int JOBTIMEOUT;
	private static TmpPath localTmp, hdfsTmp;
	private static Semaphore limit; 
	

	/**
	 * ZooKeeper event monitor for Worker's main connection.
	 */
	private static class WorkerWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			if (event.getType() == Event.EventType.None) {
				log.debug("Connected.");
			} else {
				//This happens rarely
				log.debug("SIGNAL:"
						+ " path="+event.getPath()
						+ " type="+event.getType()
				);
			}
		}
	}
	
	/**
	 * Worker's job execution thread.
	 */
	private static class Job implements Runnable {
		private JobQueue job;
		private JobData jobData;
		private FileSystem fs;

		/**
		 * Constructor.
		 * 
		 * @param job		Job queue
		 * @param jobData	Current job's information
		 * @param fs		Open HDFS handle
		 */
		public Job(JobQueue job, JobData jobData, FileSystem fs) {
			this.job = job;
			this.jobData = jobData;
			this.fs = fs;
		}

		/**
		 * Start job execution.
		 */
		@Override
		public void run() {
			String id = null;
			try {
				id = jobData.getId();
				log.debug(id + " starting a job.");
				
				Filter filter = new Filter(jobData, fs, config);
				filter.start();
				
				if (filter.await(JOBTIMEOUT) && filter.success()) {
					final Path srcFile = localTmp.asPath(id);
					final Path dstFile = hdfsTmp.asPath(id);
					try {
						log.debug(id + " moving to hdfs");
						fs.moveFromLocalFile(srcFile, dstFile);
						log.debug(id + " moving done");
					} catch (FileNotFoundException e) {
						log.debug(id + " no file, moving canceled.");
						//ignore
					}
					job.finish(id);
					log.info(id + " job finished successfully.");
				} else {
					job.failed(id);
					log.error(id + " job failed.");
				}
					
				limit.release();
			} catch (Exception e) {
				log.error("Unhandled exception: "+e.getMessage());
				e.printStackTrace();
				limit.release();
			}
		}
		
	}
	
	/**
	 * Prints CLI usage information.
	 */
	private static void printUsage() {
		System.err.printf("Usage: java -jar %s <fqdn>\n", Worker.class.getSimpleName());
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException, ConfigurationException {
		String hostname;
		JobData jobData;
		
		if (args.length < 1) {
			printUsage();
			return;
		} else {
			hostname = args[0];
		}
		
		Thread.currentThread().setName("Worker#"+new Random().nextInt(Integer.MAX_VALUE));
		
		config = new PropertiesConfiguration("nfdist.properties");
		JOBTIMEOUT = config.getInt("jobs.timeout")*1000;
		localTmp = new TmpPath(config.getString("local.path.tmp"));
		hdfsTmp = new TmpPath(config.getString("hdfs.path.tmp"));
		final int THREADS = config.getInt("jobs.filter.threads");
		
		//HDFS
		final org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		try {
			final URI hdfsURI = new URI("hdfs", config.getString("hdfs.namenode"), null, null);
			hdfsConf.set("fs.defaultFS", hdfsURI.toString());
			
			// Prevent temporary file replication
			hdfsConf.setInt("dfs.replication", 1);
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
			return;
		}
		final FileSystem fs = FileSystem.get(hdfsConf);
		
		final WorkerWatcher watcher = new WorkerWatcher();
		ReconnectingZooKeeper zk = new ReconnectingZooKeeper(config, watcher);
		JobQueue queue = new JobQueue(zk, config, hostname); 
		
		limit = new Semaphore(THREADS);
		final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
		
		while (true) {
			limit.acquire();
			try {
				jobData = queue.take();
				Job job = new Job(queue, jobData, fs);
				executor.execute(job);
			} catch (KeeperException.SessionExpiredException e) {
				//Create a new ZooKeeper session
				log.warn("ZooKeeper session expired, reconnecting...");
				zk = new ReconnectingZooKeeper(config, watcher);
				queue = new JobQueue(zk, config, hostname);
				limit.release();
				Thread.sleep(100);
			} catch (Exception e) {
				log.error("Unhandled exception: "+e.getMessage());
				e.printStackTrace();
				limit.release();
				Thread.sleep(100);
			}
		}
	}

}
