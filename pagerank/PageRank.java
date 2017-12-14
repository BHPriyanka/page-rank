package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import parser.PageLinksWritable;


/***
 * PageRankJob is the driver class for the mapreduce program to iteratively compute the page ranks for each node/page.
 */
public class PageRank {

	/** enum counter to keep track of the delta computed in the reduce phase which
	 * will be used by the next map phase in i+1 iteration
	*/
    public static enum counter{
    	delta;
    }
    
    /**String variables to dynamically track total number of nodes and delta
     * Also ISFIRST is a flag to keep track of whether its a first iteration.
     */
    public static String NUM_NODES = "Number of Nodes";
    public static String ISFIRST = "Is it first iteration";
    public static String DELTA = "Delta";

    // alpha factor initialized to 0.15
    private static double alpha=0.15;
  
    /**
     * PageRankMapper: Mapper class which acts a driver for the map phase of the page rank job.
     * @author priyanka 
     */
	public static class PageRankMapper 
	extends Mapper<Text, PageLinksWritable, Text, PageLinksWritable>{
		
		/***
		   * map : The output file of preprocessing job is parsed line by line.
		   * Assigns the initial page rank value to all the nodes during the first iteration
		   * value of pagerank = 1/total nodes
		   * Later, emits the graph representation as a whole.
		   * Checks on whether the node is dangling, if so , a special key and the new PageLinksWritable
		   * is emitted.
		   * If not, new page rank value is created, and for every outgoing links for the input node,
		   * new page rank is set.
		   * The same is emitted from mapper using context.
		   * @param key : Input key to mapper.
		   * @param value : contains the line from the input file that has the wiki data page information
		   * @param context : Used to emit output from Mapper
		   * @throws IOException
		   * @throws InterruptedException
		   * NOTE: Page Rank algorithm is heavily based on the one taught in class and given in 
		   * module.
		*/
		public void map(Text key, PageLinksWritable value, Context context)
				throws IOException, InterruptedException {
			
			double currentPageRank = 0.0;
			List<String> adjList = new ArrayList<String>();
	    	adjList = value.getLinkNames();
	    	
	    	/** To obtain the global variables/counters specifying the total number
	    	 * of  nodes and the flag to indicate whether its a first iteration.
	    	*/
	    	boolean first=context.getConfiguration().getBoolean(PageRank.ISFIRST,false);
			int nodes=(int)context.getConfiguration().getLong(PageRank.NUM_NODES,1);
				
			// initially set page rank as 1.0/total number of nodes 
		    if(first){
		    	currentPageRank=1.0/nodes;
		    	
		    }
		    
		    // Pass along the Node structure
		    PageLinksWritable graph = new PageLinksWritable();
		    graph.pageRank = currentPageRank;
		    graph.linkNames = adjList;
 	    	graph.isNode = true;
	    	context.write(key, graph);
			
	    	/** Check for a dangling node*/
		    if(adjList.size() == 0) {
		    	PageLinksWritable dangling = new PageLinksWritable();
		    	dangling.pageRank = currentPageRank;
		    	List<String> empty = new ArrayList<String>();
		    	dangling.linkNames = empty;
		    	context.write(new Text("DanglingNode"), dangling);
		    } else {
		    	//Compute contributions to send along outgoing links
		    	double p = currentPageRank / (adjList.size() * 1.0);
			   
		    	for(int i=0;i<adjList.size();i++) {
		    		PageLinksWritable outlink = new PageLinksWritable();
		    		outlink.pageRank = p;
		    		outlink.isNode = false;
		    		outlink.linkNames = new ArrayList<String>();
		    		context.write(new Text(adjList.get(i)), outlink);
		    	}
		      }
		}
	}
		
	/**
	  * PageRankReducer : Reduce task is created per page name
	 */  
	public static class PageRankReducer 
	extends Reducer<Text, PageLinksWritable, Text, PageLinksWritable> {
		 // To check the convergence of page rank values of all nodes
		double rankSum = 0.0;
		
		/**
		   * reduce: The new page ranks and delta for handling dangling nodes is compute
		   * and updated to the counter delta. And as a result of completion of this job,
		   * each page node is emitted with its new value of page ranks.
		   * @param key : pageName
		   * @param values: Contains the list of PageLinkWritable records.
		   * @param context: Used to emit output from reducer
		   * @throws IOException
		   * @throws InterruptedException
		   */
		public void reduce(Text key, Iterable<PageLinksWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			
			/** Configuration of the reducer context, to fetch the total nodes count and delta
			 * from the enum counter 
			*/
			Configuration conf = context.getConfiguration();
			int nodes =  conf.getInt(PageRank.NUM_NODES,1);
			double delta = conf.getDouble(PageRank.DELTA, 0.0);
			
			double sum = 0.0;
			double pageRank=0.0;
			List<String> adjList=new ArrayList<String>();
			
			/** Check if the entry in the values is a node, if its a node get the outgoing links
			 * If its not a node, add its page rank to the sum value which will be used later to 
			 * compute new page ranks
			 */
			for(PageLinksWritable obj: values) {
				if(obj.isNode()) {
				   adjList = obj.getLinkNames();
				} else {
					sum = sum +obj.getPageRank();
				}
			}
			
			// Compute the pagerank using the delta from i-1 iteration
			pageRank = (alpha / nodes ) + (1-alpha) * ((delta /nodes)+ sum);
						
			
			/** If the key is a dangling node object, identified by the map phase, 
			 * Set the updated page rank value to the counter delta to be used by mapper in next iteration.
			 */
			if(key.toString().equals("DanglingNode")) {
				long dang = (long)(pageRank*Math.pow(10, 9));
				context.getCounter(PageRank.counter.delta).increment(dang);			   
			}
			
			// Emit the node with its new pagerank and adjacency list
			PageLinksWritable node = new PageLinksWritable();
			node.pageRank = pageRank;
			node.linkNames = adjList;
			context.write(key, node);
			
			// To track the convergence of pageranks
			rankSum+=node.getPageRank();
		}
		
		  
		/**
		 * cleanup: To output the final sum of all page ranks for the current iteration
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		  System.out.println("Sum of ranks: " +rankSum);
				
		}
	  }
	}
