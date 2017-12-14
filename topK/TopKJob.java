package topK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import parser.PageLinksWritable;

/***
 * TopKJob is the driver class for the mapreduce program to find top 100 pages based on page ranks
 */
public class TopKJob {

	/*
	 * Order: Inner class extending the Comparator interface and defines the custom
	 * compare function to compare two PageNode records while being inserted to the data structure.
	 */
	static class Order implements Comparator<PageNode> {
		
		  /**
		   * compare: compares two PageNode objects
		   * @param o1: first PageNode object with whom it has to be compared
		   * @param o2: second PageNode object which needs to be compared against o1
		   */
		public int compare(PageNode o1, PageNode o2) {
			if(o1.pageRank > o2.pageRank) {
					return 1;
			}
			
			if(o1.pageRank < o2.pageRank) {
					return -1;
			}
			return 0;
		}
	}	



	/***
	  * TopKMapper: Mapper class which contains map function
	  * and writes  a NULL object and the PageNode as key, value pair
	  */
	public static class TopKMapper extends Mapper<Text, PageLinksWritable, NullWritable, PageNode> {
		
		//Declared priority queue
		private PriorityQueue<PageNode> rankQueue;
		
		
		/**
		 * setup: initializes the rankQueue data structure
		 * @param context: used to emit the records
		 * @throws IOException
		 * @throws InterruptedException
		 * 
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			rankQueue = new PriorityQueue<PageNode>(100, new Order());
		}


		/***
		  * map : PageNode object is created with key and page rank from the input passed to the map function
		  * The same is added to the queue.If the size of queue exceeds 100, the recently 
		  * added node is removed.
		  * @param key : Input key to mapper.
		  * @param value : contains the line from the input file that has the page node and is details
		  * for a given year.
		  * @param context : Used to emit output from Mapper
		  * @throws IOException
		  * @throws InterruptedException
		  */
		@Override
		public void map(Text key, PageLinksWritable value, Context context)
				throws IOException, InterruptedException {
	
			PageNode rankNode = new PageNode(key.toString(), value.getPageRank());
				
			if(!key.toString().equals("DanglingNode")) {
				rankQueue.add(rankNode);
			}
			
			if(rankQueue.size() > 100) {
				rankQueue.poll();
			}
			
		}

		/**
	     * cleanup : emits the accumulated page rank queue information at the end of map call.
	     * @param context : to emit records from mapper.
	     * @throws IOException
	     * @throws InterruptedException
	     */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			/* Transfer all the entries from the queue to a list, since poll function of priority queue does not
			garauntee the order of elements*/ 
	
			PageNode n = new PageNode();
			while(!rankQueue.isEmpty()) {
				n = rankQueue.poll();
				context.write(NullWritable.get(), n);
			}
			
		}
	}
	
	/**
	* TopKReducer : Reducer class which deals with the reduce phase
	*/  
	public static class TopKReducer 
	extends Reducer<NullWritable,PageNode,NullWritable, Text> {
		/** Instantiate local priority queue */
		PriorityQueue<PageNode> rankQueue = new PriorityQueue<PageNode>(100, new Order());
		
		/**
		   * reduce: The top 100 pages ranked based on their page ranks is obtained and the resultant string 
		   * containing the page name, page rank value pair is emitted.
		   * @param key : NullWritable
		   * @param values: Contains the list of PageNode records which contains the <name, rank> records.
		   * @param context: Used to emit output from reducer
		   * @throws IOException
		   * @throws InterruptedException
		 */
		public void reduce(NullWritable key, Iterable<PageNode> values, 
				Context context
				) throws IOException, InterruptedException {
			  
			    // Iterates over the input list and adds each encountered value to the queue until the size reaches 100
				for(PageNode node : values) {
					rankQueue.add(new PageNode(node.pageName, node.pageRank));
				
					if(rankQueue.size() > 100) {
						rankQueue.poll();
					}
					
				}
				
				/* Transfer all the entries from the queue to a list, since poll function of priority queue does not
				garauntee the order of elements*/ 
				List<PageNode> ll = new ArrayList<PageNode>();
				PageNode n; 
				
				while(!rankQueue.isEmpty()) {
					n = rankQueue.poll();
					ll.add(n);
				}
				
				/* Emit the top 100 pages with decreasing order of page ranks*/
				for(int i=ll.size()-1;i>=0;i--) {
					n = ll.get(i);
					context.write(NullWritable.get(), new Text(n.getPageName() +" "+n.getPageRank()));
				}
			}
	}
}
