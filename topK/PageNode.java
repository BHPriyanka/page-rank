package topK;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/***
 * PageNode: Value type class used by TopKJob classes to list out the top 100 pages based on thier page ranks.
 * Object of this class is passed as the output value from the mapper function.
 * Contains placeholders for the a flag to hold the page name and it corresponding page rank value.
 */
public class PageNode implements Writable{
	public double pageRank;
	public String pageName;
	
	public PageNode(String name, double rank){
		this.pageRank = rank;
		this.pageName = name;
	}
	
	public PageNode(){
		set("", 0.0);
	}
	
	/* Getter and setters for the fields of PageNode*/
    public String getPageName(){
    	return this.pageName;
    }
    
    public double getPageRank() {
    	return this.pageRank;
    }
   
    public void set(String name, double rank) {
    	this.pageRank = rank;
    	this.pageName = name;
    }
    

    public void readFields(DataInput in) throws IOException {
    	pageName = in.readUTF();
    	pageRank = in.readDouble();
    }
 
 
     public void write(DataOutput out) throws IOException {
    	 out.writeUTF(pageName);
    	 out.writeDouble(pageRank);
      }
     
}


