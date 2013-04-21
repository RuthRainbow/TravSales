package org.bradheintz.generalalgorithm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;

/**
 * Extension of TextInputFormat to use position in file mod number of sub-populations as key
 *
 * @author ruthking, user1707141
 * (see http://stackoverflow.com/questions/13456050/custominputformat-generates-duplicate-keys)
 */
public class KeyValueFormat extends TextInputFormat{

	@Override
    public RecordReader<LongWritable, Text> createRecordReader(
		   InputSplit split, TaskAttemptContext context) {
        return new KeyValueRecordReader();
    }

    public class KeyValueRecordReader extends RecordReader<LongWritable, Text>{
    	private LineReader in;
    	private LongWritable key;
    	private Text value = new Text();
    	private long start =0;
    	private long end =0;
    	private long pos =0;
    	private int maxLineLength;
    	private int numSubPops;

		@Override
		public void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float)(end - start));
			}
		}

		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			 FileSplit split = (FileSplit) genericSplit;
			 final Path file = split.getPath();
			 Configuration conf = context.getConfiguration();
			 this.numSubPops = conf.getInt("numSubPopulations", 10);
			 this.maxLineLength = conf.getInt(
					 "mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
			 FileSystem fs = file.getFileSystem(conf);
			 start = split.getStart();
			 end= start + split.getLength();
			 boolean skipFirstLine = false;
			 FSDataInputStream filein = fs.open(split.getPath());

			 if (start != 0){
				 skipFirstLine = true;
				 --start;
				filein.seek(start);
			 }
			 in = new LineReader(filein,conf);
			 if(skipFirstLine){
			 	start += in.readLine(
			 			new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
			 }
			 this.pos = start;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (key == null) {
				key = new LongWritable();
			}
			// Here the key is set as the position in file mod the number of subpopulations
			key.set(pos%numSubPops);
			if (value == null) {
				value = new Text();
			}
			value.clear();
			final Text endline = new Text("\n");
			int newSize = 0;
			Text v = new Text();
			while (pos < end) {
				newSize = in.readLine(v, maxLineLength,Math.max(
						(int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
				value.append(v.getBytes(),0, v.getLength());
				value.append(endline.getBytes(),0, endline.getLength());
				if (newSize == 0) {
					break;
				}
				pos += newSize;
				if (newSize < maxLineLength) {
					break;
				}
			}
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

    }
}