package parser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;

/***
 * PageLinksWritable: Value type class used by PreProcessingJob, PageRankJob and TopKJob classes.
 * Object of this class is passed as the output value from the mapper and reducer functions of PreProcessingJob, .
 * input and output value for the mapper function on PageRankJob, and input value to the TopKJob.
 * Contains placeholders for the a flag to distinguish between a node or just the page rank being sent in PageRankJob,
 * and the page rank value, and the adjacency list of each node of the graph. 
 */
public class PageLinksWritable implements Writable{
		public List<String> linkNames;
		public double pageRank;
		public boolean isNode;
		
		public PageLinksWritable(){
			set(0.0, false);
		}
		
		/* getter methods to fetch the adjacency list, page rank and the type of node */
	    public List<String> getLinkNames(){
	    	return this.linkNames;
	    }
	    
	    public double getPageRank() {
	    	return this.pageRank;
	    }
	    
	    public boolean isNode() {
	    	return this.isNode;
	    }
	    
	    /* Default setter method to initialise the fields of PageLinksWritable class*/
	    public void set(double rank, boolean node) {
	    	this.pageRank = rank;
	    	this.isNode = node;
	    }
	    

	    public void readFields(DataInput in) throws IOException {
	    	pageRank = in.readDouble();
	    	
	        this.linkNames = new ArrayList<String>();
	        int n = in.readInt();
	        	        
	        for(int i = 0; i < n; i++) {
	            this.linkNames.add(in.readUTF());
	        }
	        isNode = in.readBoolean();
	    }
	 
	 
	     public void write(DataOutput out) throws IOException {
	    	 out.writeDouble(pageRank);
	    	 out.writeInt(linkNames.size());
	         for(String page: linkNames) {
	             out.writeUTF(page);
	         } 
	         out.writeBoolean(isNode);
	    }
	     
}


