package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import pagerank.PageRank;
import pagerank.PageRank.PageRankMapper;
import pagerank.PageRank.PageRankReducer;
import parser.PageLinksWritable;
import parser.PreProcessingJob;
import parser.PreProcessingJob.PreProcessingMapper;
import parser.PreProcessingJob.PreProcessingMapper.counter;
import topK.PageNode;
import topK.TopKJob;
import topK.TopKJob.TopKMapper;
import topK.TopKJob.TopKReducer;
import parser.PreProcessingJob.PreProcessingReducer;

public class DriverProgram {

   /***
	  * Main : Setups up the mapreduce environment configuration.
	  * This program involves only mapper and reducer for all the three jobs
	  * Job 1: PreProcessingJob
	  * Job 2: PageRankJob 
	  * Job 3: TopKJob
	  * Input file name and output directory configuration is read from args
	  * @param args : contains two parameters input file name and the output directory folder for the PreProcessing phase.
	  */
	public static void main(String[] args) throws Exception {
	   
		DriverProgram dr = new DriverProgram();
		
		/**Define a configuration for the PreProcessingJob */
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    
	    if (otherArgs.length < 4) {
	    	System.err.println("Usage: PreProcessingJob <preProcessorInput> <preProcessorOutput> <pageRankOutput> <topKoutput>");
	    	System.exit(2);
	    }
	    
	    // Run the pre processing on the input data and get the total number of page nodes
		long preProcess_startTime = System.currentTimeMillis();
	    long totalNodes = dr.preProcessData(conf, otherArgs);
	    long preProcess_endTime = System.currentTimeMillis();
		System.out.println("Execution time of PreProcessingJob: " +(preProcess_endTime-preProcess_startTime));
		
	    // Initialize the number of iterations before running the PageRank mapreduce job
		int iterations = 10;
		dr.pageRankComputation(otherArgs, iterations,totalNodes);
	    	     
	    // Run the TopKJob and fetch the top 100 pages in the given data ranked in descending order of thier page ranks
	    long top_startTime = System.currentTimeMillis();
	    dr.top100Pages(iterations, otherArgs);
	    long top_endTime = System.currentTimeMillis();
		System.out.println("Execution time of Top 100 Job: " +(top_endTime-top_startTime));
	}
	
	/**
	 * preProcessingData: Method to run parsing and pre processing on the huge input files
	 * @param conf: Configuration for pre processing job
	 * @param otherArgs: commandline arguments given while running the program
	 * @return: total number of page nodes found afer parsing
	 * @throws Exception
	 */
	public long preProcessData(Configuration conf, String[] otherArgs) throws Exception{
		 
		    Job preProcessJob = Job.getInstance(conf, "PreProcessor");
		    preProcessJob.setJarByClass(PreProcessingJob.class);
		    preProcessJob.setMapperClass(PreProcessingMapper.class);
		    preProcessJob.setReducerClass(PreProcessingReducer.class);
		    preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);
	    	
		    preProcessJob.setOutputKeyClass(Text.class);
		    preProcessJob.setOutputValueClass(PageLinksWritable.class);
			
		    preProcessJob.setMapOutputKeyClass(Text.class);
		    preProcessJob.setMapOutputValueClass(PageLinksWritable.class);
			    
			FileInputFormat.addInputPath(preProcessJob, new Path(otherArgs[0]));	
			FileOutputFormat.setOutputPath(preProcessJob, new Path(otherArgs[1]));
			
			preProcessJob.waitForCompletion(true);
			long total = preProcessJob.getCounters().findCounter(counter.numOfNodes).getValue();
			return total;
	}
	
	/**
	 * pageRankComputation: Method to run PageRankJob ten times, computes page ranks taking dangling nodes into consideration
	 * @param otherArgs: Commandline arguments given while running the program
	 * @param iterations: Number of times this job needs to be executed
	 * @param totalPages: Total number of pages which is used to initiaize the page rank of all the nodes to 1/totalPages
	 * @throws Exception
	 */
	public void pageRankComputation(String[] otherArgs, int iterations, long totalPages) throws Exception{
		double delta =0.0;
		Configuration pageRankConf = new Configuration();
		Path inPath = new Path(otherArgs[1]);
	    Path outPath = null;
	        
	    for(int i=0; i<10; i++) {
	    	long startTime = System.currentTimeMillis();
	    	pageRankConf.setLong(PageRank.NUM_NODES,totalPages);
	    	
	        if(i==0){
	        	pageRankConf.setBoolean(PageRank.ISFIRST,true);
	        }
	        else{
	        	pageRankConf.setBoolean(PageRank.ISFIRST,false);
	        }
	        
	        pageRankConf.setDouble(PageRank.DELTA,delta);
	        
	    	Job pageRankJob = Job.getInstance(pageRankConf, "Page Rank Job");
	    	pageRankJob.setJarByClass(PageRank.class);
	    	pageRankJob.setMapperClass(PageRankMapper.class);
	    	pageRankJob.setReducerClass(PageRankReducer.class);
	   	    	
	    	pageRankJob.setInputFormatClass(SequenceFileInputFormat.class);
	    	pageRankJob.setOutputFormatClass(SequenceFileOutputFormat.class);
	    		    	
	    	pageRankJob.setOutputKeyClass(Text.class);
	    	pageRankJob.setOutputValueClass(PageLinksWritable.class);
		  
	    	FileInputFormat.addInputPath(pageRankJob, inPath);
		
	    	outPath = new Path(otherArgs[2]+i);
	    	FileOutputFormat.setOutputPath(pageRankJob, outPath);
	    	pageRankJob.waitForCompletion(true);
	    	
	    	delta=Double.longBitsToDouble(pageRankJob.getCounters().findCounter(PageRank.counter.delta).getValue());
	    	inPath = outPath;
	    	long endTime = System.currentTimeMillis();
	 		System.out.println("Execution time of PageRankJob: " +(endTime-startTime));
	    }
	}
	
	/**
	 * top100Pages: Method to get the top 100 page nodes based on their page rank values
	 * @param iterations: Total number of iterations the page rank job was run, used to read input from the last written folder 
	 * @param otherArgs: Commandline arguments given while running the driver program
	 * @throws Exception
	 */
	public void top100Pages(int iterations, String[] otherArgs) throws Exception{
		Configuration topKConf = new Configuration();
	  	  
	    Job topKJob = Job.getInstance(topKConf, "Top K pages");
	    topKJob.setJarByClass(TopKJob.class);
	    topKJob.setMapperClass(TopKMapper.class);
	   	topKJob.setReducerClass(TopKReducer.class);
	   	
	    topKJob.setInputFormatClass(SequenceFileInputFormat.class);

	    topKJob.setMapOutputKeyClass(NullWritable.class);
	    topKJob.setMapOutputValueClass(PageNode.class);
	    
	    topKJob.setOutputKeyClass(NullWritable.class);
	    topKJob.setOutputValueClass(Text.class);
		    
	    FileInputFormat.addInputPath(topKJob, new Path(otherArgs[2]+(iterations-1)));
		FileOutputFormat.setOutputPath(topKJob,
				new Path(otherArgs[3]));
		topKJob.waitForCompletion(true);
	}
}
