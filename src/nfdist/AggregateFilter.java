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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Provides methods to determine which tags are necessary depending on filter,
 * statistics type and given aggregate tags.  
 */
public class AggregateFilter {
	private static final Logger log = Logger.getLogger(AggregateFilter.class);
	
	private boolean proto=false;
	private boolean srcip=false, dstip=false;
	private boolean srcport=false, dstport=false;
	
	/**
	 * Check if any of the aggregate tags is set
	 */
	public boolean isTagSet() {
		return (proto || srcip || dstip || srcport || dstport); 
	}

	/**
	 * Reset all the aggregate tags
	 */
	public void resetTags() {
		proto=srcip=dstip=srcport=dstport=false;
	}
	
	/**
	 * Get the list of needed aggregation tags
	 * 
	 * @return	String of comma separated tags
	 */
	public String getTags() {
		List<String> tags = new ArrayList<String>();
		if (proto) tags.add("proto");
		if (srcip) tags.add("srcip");
		if (dstip) tags.add("dstip");
		if (srcport) tags.add("srcport");
		if (dstport) tags.add("dstport");
		return StringUtils.join(tags, ",");
	}
	

	/**
	 * Parses aggregation tag list supplied with nfdump's -A option
	 * 
	 * @param	opt	Comma separated tag list 
	 */
	public void parseTags(String opt) {
		String opts = opt.trim();
		if (opts.isEmpty()) {
			return;
		}
		String[] tags = opts.split(",");
		for (String tag: tags) {
			tag = tag.toLowerCase();
			if (tag.equals("proto")) proto=true;
			else if (tag.equals("srcip")) srcip=true;
			else if (tag.equals("dstip")) dstip=true;
			else if (tag.equals("srcport")) srcport=true;
			else if (tag.equals("dstport")) dstport=true;
			else {
				log.warn("Skipping unhandled tag: " + tag);
			}
		}
	}
	
	/**
	 * Parses filter string and sets necessary aggregation tags 
	 * 
	 * @param	filter	a string to be parsed
	 */
	public void parseFilter(String filter) {
		boolean src=false, dst=false;
		String[] primitives = filter.split(" ");
		
		for (String primitive: primitives) {
			primitive = primitive.toLowerCase();
			
			if (primitive.equals("proto")) proto=true;
			else if (primitive.equals("host")) {
				if (true == src) {
					srcip=true;
				}
				else if (true == dst) {
					dstip=true;
				}
				else {
					srcip=dstip=true;
				}
			}
			else if (primitive.equals("port")) {
				if (true == src) {
					srcport=true;
				}
				else if (true == dst) {
					dstport=true;
				}
				else {
					srcport=dstport=true;
				}
			}
			else if (primitive.equals("src")) {
				src=true;
				continue;
			}
			else if (primitive.equals("dst")) {
				dst=true;
				continue;
			}
			
			src=dst=false;
		}
	}
	
	/**
	 * Parse statistics type (nfdump's -s option) and set appropriate tags
	 * 
	 * @param	opt	Option string in format "type/order"
	 */
	public void parseStats(String opt) {
		try {
			if (opt.trim().length() > 0) {
				String type = opt.split("/")[0].toLowerCase();
				if (type.equals("proto")) proto=true;
				else if (type.equals("record")) srcip=dstip=true;
				else if (type.equals("ip")) srcip=dstip=true;
				else if (type.equals("srcip")) srcip=true;
				else if (type.equals("dstip")) dstip=true;
				else if (type.equals("port")) srcport=dstport=true;
				else if (type.equals("srcport")) srcport=true;
				else if (type.equals("dstport")) dstport=true;
			}
		} catch (Exception e) {
			//ignore
		}
	}
}
