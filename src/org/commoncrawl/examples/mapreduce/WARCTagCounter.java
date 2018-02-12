package org.commoncrawl.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.warc.WARCFileInputFormat;

/**
 * HTML tag count example using the raw HTTP responses (WARC) from the Common Crawl dataset.
 *
 * @author Stephen Merity (Smerity)
 */
public class WARCTagCounter extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(WARCTagCounter.class);

	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the Hadoop job.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WARCTagCounter(), args);
		System.exit(res);
	}

	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the Hadoop job.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//
		Job job = new Job(conf);
		job.setJarByClass(WARCTagCounter.class);
		job.setNumReduceTasks(1);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(WARCFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(TagCounterMapper.class);
		job.setReducerClass(LongSumReducer.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}


}
