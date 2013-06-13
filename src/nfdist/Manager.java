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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import nfdist.JobProto.JobInfo;
import nfdist.hadoop.FileList;
import nfdist.zookeeper.JobRequest;
import nfdist.zookeeper.ReconnectingZooKeeper;

/**
 * Manager submits job requests, processes the results and provides the final output.
 */
public class Manager {
	private static final Logger log = Logger.getLogger(Manager.class);
	private static JobRequest jobs;
	

	/**
	 * ZooKeeper event monitor.
	 */
	private static class ManagerWatcher implements Watcher {

		/**
		 * Monitors states of submitted jobs.
		 */
		@Override
		public void process(WatchedEvent event) {
			String path = event.getPath();
			EventType type = event.getType();
			
			switch (type) {
				case None:
					log.debug("Connected.");
					break;
				case NodeDataChanged:
					try {
						jobs.completeIfFinished(path);
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;
				case NodeCreated:
					if (jobs.isRegistered(path)) {
						log.debug("My job was taken: "+path);

						// Activate the watch
						try {
							jobs.completeIfFinished(path);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					break;
				default:
					//log.debug("SIGNAL:"
					//		+ " path="+path
					//		+ " type="+type
					//		);
					break;
			}
		}
	}

	/**
	 * Creates job for each file in the list.
	 * 
	 * @param jobs		Job queue
	 * @param files		List of files to process
	 * @param args		Nfdump arguments
	 * @param filter	Netflow filter string
	 * @return	Number of jobs activated
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static int submitJobs(JobRequest jobs, FileList files, List<String> args, String filter) throws IOException, KeeperException, InterruptedException {
		int count = 0;
		final List<FileStatus> stats = files.getStats();
		for (FileStatus s: stats) {
			if (jobs.isCanceled()) {
				break;
			}
			JobInfo.Builder job = JobInfo.newBuilder();
			job.setNfFile(s.getPath().toUri().getPath());
			job.addAllServer(files.getServers(s));
			job.setFilter(filter);
			job.addAllArgs(args);
			
			jobs.create(job.build().toByteArray());
			count++;
		}
		log.debug("Jobs submitted: " + count + "/" + stats.size());
		return count;
	}
	
	public static void main(String[] args) throws ConfigurationException, IOException, KeeperException, InterruptedException, IllegalArgumentException, MissingArgumentException {
		final long start = System.currentTimeMillis();
		long stop;

		Thread.currentThread().setName("Manager#"+new Random().nextInt(Integer.MAX_VALUE));
		
		final Configuration config = new PropertiesConfiguration("nfdist.properties");
		final int JOBTIMEOUT = config.getInt("jobs.timeout")*1000;
		
		// Options
		final Options opts = new Options(config);
		opts.parse(args);
		
		final String filter = opts.getFilter();
		final List<String> allArgs = opts.getAllArgs();
		
		//If no files are accessed (e.g. filter syntax check)
		if (opts.noFiles()) {
			NoFiles nofiles = new NoFiles(config);
			nofiles.start(allArgs, filter);  
			nofiles.await(JOBTIMEOUT);
			stop = System.currentTimeMillis();
			log.info("+" + (stop-start)/1000f + "s. \tfinished.");
			return;
		}
		
		// More options
		final String path = config.getString("hdfs.path.root") + "/" + opts.getPath();
		final String[] idents = opts.getIdents();
		final Date startDate = opts.getStart();
		final Date endDate = opts.getEnd();
		
		//HDFS
		final org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
		try {
			final URI hdfsURI = new URI("hdfs", config.getString("hdfs.namenode"), null, null);
			hdfsConf.set("fs.defaultFS", hdfsURI.toString());
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
			return;
		}
		final FileSystem fs = FileSystem.get(hdfsConf);
		
		final FileList files = new FileList(fs, path, idents, startDate, endDate);
		
		if (files.oneFile()) {
			final Direct direct = new Direct(fs, files, config);
			
			direct.start(allArgs, filter);
			direct.await(JOBTIMEOUT);
			stop = System.currentTimeMillis();
			log.info("+" + (stop-start)/1000f + "s. \tcombiner finished.");
			
		} else {
			// More options
			//final boolean doStats = opts.doStats();
			final List<String> combinerArgs = opts.getCombinerArgs();
			final List<String> workerArgs = opts.getWorkerArgs();
			
			//ZooKeeper
			final ManagerWatcher watcher = new ManagerWatcher();
			final ReconnectingZooKeeper zk = new ReconnectingZooKeeper(config, watcher);

			jobs = new JobRequest(zk, config);
			
			//Result combiner thread
			Combiner comb = new Combiner(fs, jobs, config);
			comb.start(combinerArgs, filter);

			try {
				final int numOfJobs = submitJobs(jobs, files, workerArgs, filter);
				if (0 == numOfJobs) {
					comb.stop();
				}
			} catch (IOException e) {
				log.error(e.getMessage());
				comb.stop();
			}
					
			//--- Time info ---
			stop = System.currentTimeMillis();
			log.info("+" + (stop-start)/1000f + "s. \tcompleted job submission.");
			
			if (!jobs.await(JOBTIMEOUT)) {
				log.error("Timeout while waiting for all jobs to finish!");
				comb.await(1000);
			} else {
				stop = System.currentTimeMillis();
				log.info("+" + (stop-start)/1000f + "s. \tcompleted " + jobs.getCompletedCount() + " job(s).");
				
				if (!comb.await(JOBTIMEOUT)) {
					log.error("Timeout while waiting for combiner to finish!");
				} else {
					stop = System.currentTimeMillis();
					log.info("+" + (stop-start)/1000f + "s. \tcombiner finished.");
				}
			}
			zk.close();
		}
		fs.close();
	}
}
