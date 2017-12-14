package parser;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/***
 * PreProcessingJob is the driver class for the mapreduce program for pre processing the input Wiki data
 */
public class PreProcessingJob {
	
	/***
	* PreProcessingMapper: Mapper class which contains map function
	* which produces pageName,PageLinksWritable object as the key,value
	*/
	public static class PreProcessingMapper 
	extends Mapper<Object, Text, Text, PageLinksWritable>{
	
		/** Hadoop built in functionality of a counter to keep track of the total number of nodes/pages */
		public static enum counter{
			numOfNodes;
		}
			
		private static Pattern namePattern;
		
		static {
			/* Keep only html pages not containing tilde (~).
			 * Keep only html filenames ending relative paths and not containing tilde (~).
			 */
			namePattern = Pattern.compile("^([^~]+)$");
		}


		/***
		   * map : The input compressed file is parsed using the SAX parser and only the page names within the bodyContent
		   * tag is retrieved based on pre determined conditions.
		   * The same is emitted from mapper using context.
		   * @param key : Input key to mapper.
		   * @param value : contains the line from the input file that has the wiki data page information
		   * @param context : Used to emit output from Mapper
		   * @throws IOException
		   * @throws InterruptedException
		*/
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			boolean isNotValid = false;
			String pageName = null;
			
			try {
				// 	Configure parser.
				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				SAXParser saxParser = spf.newSAXParser();
				XMLReader xmlReader = saxParser.getXMLReader();
				
				// Parser fills this list with linked page names.
				List<String> linkPageNames = new LinkedList<>();
				xmlReader.setContentHandler(new WikiParser(linkPageNames));
				
				// Each line formatted as (Wiki-page-name:Wiki-page-html).
				int delimLoc = value.toString().indexOf(':');
				pageName = value.toString().substring(0, delimLoc);
				String html = value.toString().substring(delimLoc + 1);
				html = html.replace("&","@amp;");
				Matcher matcher = namePattern.matcher(pageName);
				if (!matcher.find()) {
					// Skip this html file, name contains (~).
					isNotValid = true; 
				} 
				

				// Parse page and fill list of linked pages.
				linkPageNames.clear();
				try {
					xmlReader.parse(new InputSource(new StringReader(html)));
				} catch (Exception e) {
					// 	Discard ill-formatted pages.
					isNotValid = true;
				}
					if(!isNotValid) {
						// increment the number of pages encountered by 1
						context.getCounter(counter.numOfNodes).increment(1);
						
						// create a new PageLinksWritable object with the updated adjacency list
						// emit the same
						PageLinksWritable pageLinks = new PageLinksWritable();
						pageLinks.linkNames = linkPageNames;
						context.write(new Text(pageName), pageLinks);
							
					}
				
		} catch (Exception e) {
			e.printStackTrace();
		} 

		}
	}
		
	 /**
	   * PreProcessingReducer : Reduce task is created per page name
	   */  
	public static class PreProcessingReducer 
	extends Reducer<Text,PageLinksWritable,Text, PageLinksWritable> {
				
		/**
		   * reduce: The outgoing links of every key node is accumulated in a a list.
		   * At the end of the reduce phase, pagename and its corresponding adjacency list is given out as result.
		   * @param key : pageName
		   * @param values: Contains the list of PageLinkWritable records.
		   * @param context: Used to emit output from reducer
		   * @throws IOException
		   * @throws InterruptedException
		   */
		public void reduce(Text key, Iterable<PageLinksWritable> values, 
				Context context
				) throws IOException, InterruptedException {
				
					
			/* Iterate over the page node records for the given page name key.
		  	 * and accumulate in a list.
		  	 */
			List<String> links = new ArrayList<String>();
			for(PageLinksWritable val : values) {
					links.addAll(val.getLinkNames());
			}
		
			PageLinksWritable pageLinks = new PageLinksWritable();
			pageLinks.linkNames = links;
			context.write(key, pageLinks);
		}

	}
}

