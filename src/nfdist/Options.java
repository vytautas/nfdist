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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

/**
 * Parse nfdump arguments.
 */
public class Options {
	private static final Logger log = Logger.getLogger(Options.class);
	private final List<String> filterArgs = new ArrayList<String>();
	private final List<String> combinerArgs = new ArrayList<String>();
	private final List<String> allArgs = new ArrayList<String>();
	private final SimpleDateFormat format = new SimpleDateFormat("'nfcapd.'yyyyMMddHHmm");
	private final String prefix;
	private String idents="", filter="", path="";
	private Date start=null, end=null;
	private boolean noFiles=false;
	private boolean doStat=false;
	private String statsType="";
	private boolean doAggregate=false;
	private String aggregateTags="";
	
	/**
	 * Convert netflow data filename to date.
	 * 
	 * @param path	full path to the file
	 * @return Date
	 * @throws ParseException
	 */
	private Date pathToDate(String path) throws ParseException {
		String file = path.substring(path.lastIndexOf("/")+1);
		return format.parse(file);
	}
	
	/**
	 * Constructor.
	 * 
	 * @param config	Nfdist's configuration
	 */
	public Options(Configuration config) {
		prefix = config.getString("local.path.datadir");
	}
	
	/**
	 * Parse given nfdump's arguments
	 * 
	 * @param args	Nfdump arguments
	 * @throws IllegalArgumentException
	 * @throws MissingArgumentException
	 */
	public void parse(String args[]) throws IllegalArgumentException, MissingArgumentException {
		
		if (args.length == 0) {
			throw new MissingArgumentException("No nfdump arguments were given.");
		}
		
		for (int i=0; i<args.length; i++) {
			if (args[i].charAt(0) == '-') {
				switch (args[i].charAt(1)) {
					//Handled options
					case 'r':
						try {
							start = pathToDate(args[++i]);
							end = start;
						} catch (ParseException e) {
							throw new IllegalArgumentException("Invalid file format for option -r");
						}
						break;
					case 'R':
						String[] parts = args[++i].split(":");
						try {
							start = pathToDate(parts[0]);
							end = pathToDate(parts[1]);
						} catch (ParseException e) {
							throw new IllegalArgumentException("Invalid file format for option -R");
						}
						break;
					case 'M':
						final int idx=args[i+1].lastIndexOf("/");
						idents = args[i+1].substring(idx+1);
						path = args[i+1].substring(0,idx);
						if (path.startsWith(prefix)) {
							path = path.substring(prefix.length()+1, idx);
							if (path.charAt(0) == '/') {
								path = path.substring(1);
							}
						}
						i++;
						break;
						
					//Unhandled single argument options
					case 'I':
					case 'q':
					case 'N':
					case 'T':
					case 'z':
					case '6':
						combinerArgs.add(args[i]);
						allArgs.add(args[i]);
						break;
					case 'm':
					case 'a':
					case 'b':
					case 'B':
						filterArgs.add(args[i]);
						combinerArgs.add(args[i]);
						allArgs.add(args[i]);
						break;
						
					//Unhandled multiple argument options
					case 't':
					case 'l':
					case 'L':
						filterArgs.add(args[i]);
						filterArgs.add(args[i+1]);
						allArgs.add(args[i]);
						allArgs.add(args[i+1]);
						i++;
						break;
					case 's':
						doStat=true;
						statsType=args[i+1];
						combinerArgs.add(args[i]);
						combinerArgs.add(args[i+1]);
						allArgs.add(args[i]);
						allArgs.add(args[i+1]);
						i++;
						break;
					case 'A':
						//Additional tags might be needed for Worker to
						//prevent data loss when combining. They are added
						//after the filter is set.
						doAggregate=true;
						aggregateTags=args[i+1];
						combinerArgs.add(args[i]);
						combinerArgs.add(args[i+1]);
						allArgs.add(args[i]);
						allArgs.add(args[i+1]);
						i++;
						break;
					case 'w':
					case 'D':
					case 'O':
					case 'n':
					case 'o':
						combinerArgs.add(args[i]);
						combinerArgs.add(args[i+1]);
						allArgs.add(args[i]);
						allArgs.add(args[i+1]);
						i++;
						break;
					case 'c':
						filterArgs.add(args[i]);
						filterArgs.add(args[i+1]);
						combinerArgs.add(args[i]);
						combinerArgs.add(args[i+1]);
						allArgs.add(args[i]);
						allArgs.add(args[i+1]);
						i++;
						break;
						
					//Ignored args
						case 'Z':
						case 'X':
						case 'V':
						case 'h':
							noFiles=true;
							combinerArgs.add(args[i]);
							allArgs.add(args[i]);
							break;
						case 'f':
						case 'i':
						case 'v':
						case 'x':
						case 'j':
							log.info("Skipping argument "+args[i]+" "+args[++i]);
							break;
							
					//Other unknown args
					default:
						throw new IllegalArgumentException("Argument '"+args[i]+"' is not supported");
				}
			} else {
				filter=filter.concat(" "+args[i]);
			}
		}
		filter=filter.replaceAll("\\n", " ").trim();
		
		//Worker might need additional aggregation tags.
		if (doAggregate || doStat) {
			AggregateFilter af = new AggregateFilter();
			af.parseTags(aggregateTags);
			af.parseStats(statsType);
			af.parseFilter(filter);
			if (af.isTagSet()) {
				filterArgs.add("-A");
				filterArgs.add(af.getTags());
			}
		}
		
		if (!noFiles && start == null) {
			throw new MissingArgumentException("Please specify a data file");
		}
	}

	/**
	 * @return Netflow filter string.
	 */
	public String getFilter() {
		return filter;
	}
	
	/**
	 * @return List of sources to process.
	 */
	public String[] getIdents() {
		return idents.split(":");
	}
	
	/**
	 * @return Profile path.
	 */
	public String getPath() {
		return path;
	}
	
	/**
	 * @return Processing period start.
	 */
	public Date getStart() {
		return start;
	}
	
	/**
	 * @return Processing period end.
	 */
	public Date getEnd() {
		return end;
	}
	
	/**
	 * @return Modified list of arguments tailored for the Worker.
	 */
	public List<String> getWorkerArgs() {
		return filterArgs;
	}
	
	/**
	 * @return Modified list of arguments tailored for the Combiner.
	 */
	public List<String> getCombinerArgs() {
		return combinerArgs;
	}
	
	/**
	 * @return Original list of arguments.
	 */
	public List<String> getAllArgs() {
		return allArgs;
	}
	
	/**
	 * @return True if no files are to be processed.
	 */
	public boolean noFiles() {
		return noFiles;
	}
}
