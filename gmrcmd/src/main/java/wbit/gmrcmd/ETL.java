package wbit.gmrcmd;

import com.google.gson.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ETL 
{
	public static class ETLMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Gson gson = new Gson();
				String[] tuple = value.toString().split("\\n");
				for (int i = 0; i < tuple.length; i++) {
					JsonObject obj = gson.fromJson(tuple[i], JsonObject.class);
					for (Map.Entry<String,JsonElement> entry : obj.entrySet()) {
						context.write(new IntWritable(entry.getValue().getAsJsonArray().size()), new Text(entry.getKey()));
					}
				}
			} catch (JsonParseException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class ETLReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			for (Text id : value) {
				context.write(key, id);
			}
		}
	}
	
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();

        Job job = new Job(conf, "ETL");
        job.setJarByClass(ETL.class);
        job.setMapperClass(ETLMapper.class);
        job.setReducerClass(ETLReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
