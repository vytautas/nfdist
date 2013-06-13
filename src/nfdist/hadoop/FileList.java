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

package nfdist.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Manage netflow file list with additional information.
 */
public class FileList {
    private final FileSystem fs;
    private final SimpleDateFormat fileFormat, dirFormat;
    private final String root;
    private final String[] idents;
    private final Date start, end;

	/**
	 * Filters netflow files by date
	 */
	private class MyPathFilter implements PathFilter {
		private final Calendar startFull, startDay, endFull, endDay;
		
		/**
		 * Constructor.
		 * 
		 * @param start		Netflow analysis range start
		 * @param end		Netflow analysis range end
		 */
		public MyPathFilter(Date start, Date end) {
			startFull = Calendar.getInstance();
			startFull.setTime(start);
			
			endFull = Calendar.getInstance();
			endFull.setTime(end);
			
			startDay = Calendar.getInstance();
			startDay.setTime(start);
			startDay.set(Calendar.HOUR_OF_DAY, 0);
			startDay.set(Calendar.MINUTE, 0);
			startDay.set(Calendar.SECOND, 0);
			startDay.set(Calendar.MILLISECOND, 0);
			
			endDay = Calendar.getInstance();
			endDay.setTime(end);
			endDay.set(Calendar.HOUR_OF_DAY, 0);
			endDay.set(Calendar.MINUTE, 0);
			endDay.set(Calendar.SECOND, 0);
			endDay.set(Calendar.MILLISECOND, 0);
		}

		/**
		 * Check if file/dir is in within date range 
		 */
		@Override
		public boolean accept(Path path) {
			final String name = path.getName();
			Calendar date = Calendar.getInstance();
			
			if (name.startsWith("nfcapd")) {
				try {
					date.setTime(fileFormat.parse(name));
					if (date.before(startFull) || date.after(endFull)) {
						return false;
					}
				} catch (ParseException e) {
					return false;
				}
			} else {
				try {
					date.setTime(dirFormat.parse(name));
					if (date.before(startDay) || date.after(endDay)) {
						return false;
					}
				} catch (ParseException e) {
					return false;
				}	
			}
			return true;
		}
	}
	
	/**
	 * Constructor.
	 * 
	 * @param fs		Open HDFS handle
	 * @param root		List from this directory
	 * @param idents	Array of sources
	 * @param start		Acceptable file range start date
	 * @param end		Acceptable file range end date
	 * @throws IOException
	 */
	public FileList(FileSystem fs, String root, String[] idents, Date start, Date end) throws IOException {
	    this.fs = fs;
	    this.root = root;
	    this.idents = idents;
	    this.start = start;
	    this.end = end;
	    this.fileFormat = new SimpleDateFormat("'nfcapd.'yyyyMMddHHmm");
	    this.dirFormat = new SimpleDateFormat("yyyy-MM-dd");
	}

	/**
	 * Get servers names which store the first block.
	 * @param status	file's status object
	 * @return	Server list
	 * @throws IOException
	 */
	public List<String> getServers(FileStatus status) throws IOException {
		//Only first block is considered.
		BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, 0);
		if (blocks.length > 0) {
			List<String> hosts = new ArrayList<String>(Arrays.asList(blocks[0].getHosts()));
			return hosts;
		} else {
			return null;
		}
	}
	
	/**
	 * Get status objects of matching files
	 * @return
	 * @throws IOException
	 */
	public List<FileStatus> getStats() throws IOException {
		final MyPathFilter filter = new MyPathFilter(start, end);
		final List<FileStatus> list = new ArrayList<FileStatus>();
		final Queue<String> dirs = new LinkedList<String>();
		Path path;
		
		//Initial directories to parse
		for (String ident: idents) {
			dirs.add(root+"/"+ident);
		}
		
		String dir;
		while (!dirs.isEmpty()) {
			dir = dirs.poll();
			path = new Path(dir);
			FileStatus[] stats = fs.listStatus(path, filter);
			for (FileStatus s : stats) {
				if (s.isDirectory()) {
					dirs.add(dir+"/"+s.getPath().getName());
				} else {
					list.add(s);
				}
			}
		}
		return list;
	}

	/**
	 * @return	True if there is only one file to be analyzed
	 */
	public boolean oneFile() {
		if (end.equals(start) && idents.length == 1) {
			return true;
		}
		return false;
	}
}
