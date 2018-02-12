package org.commoncrawl.examples.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.martinkl.warc.WARCWritable;

public class TagCounterMapper extends Mapper<LongWritable, WARCWritable, Text, LongWritable> {
		private Text outKey = new Text();
	private static final Logger LOG = Logger.getLogger(TagCounterMapper.class);
		private LongWritable outVal = new LongWritable(1);
		// The HTML regular expression is case insensitive (?i), avoids closing tags (?!/),
		// tries to find just the tag name before any spaces, and then consumes any other attributes.
		private static final String HTML_TAG_PATTERN = "(?i)<(?!/)([^\\s>]+)([^>]*)>";
	private static final String EMAIL_PATTERN = "[a-zA-Z0-9]+[._a-zA-Z0-9!#$%&'*+-/=?^_`{|}~]*[a-zA-Z]*@[a-zA-Z0-9]{2,8}.[a-zA-Z.]{2,6}";
		private Pattern patternTag;
		private Matcher matcherTag;

		@Override
		public void map(LongWritable key, WARCWritable value, Context context) throws IOException {
			// Compile the regular expression once as it will be used continuously
			patternTag = Pattern.compile(EMAIL_PATTERN,Pattern.CASE_INSENSITIVE);
			LOG.info("In map task ossdfdsgsfnjdzgsdgdbhfgsdfghrdgr");
			String recordType = value.getRecord().getHeader().getRecordType();
			String targetURL  = value.getRecord().getHeader().getTargetURI();
			String body = new String(value.getRecord().getContent());
//			LOG.info(body);
//			LOG.info("In map task ossdfdsgsfnjdzgsdgdbhfgsdfghrdgr"+targetURL);
			patternTag = Pattern.compile(EMAIL_PATTERN);
			matcherTag = patternTag.matcher(body);
			while (matcherTag.find()) {
				LOG.info("matching data" +matcherTag.group());
				try {
					context.write(new Text(matcherTag.group(0)),outVal);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
}
