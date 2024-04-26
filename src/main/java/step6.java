import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class step6 {

    public static class MapperClass extends Mapper<LongWritable, Text, ComparableKey, Text> {
        
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] split = line.toString().split("\t");
            String npmi = split[1];
            String [] split_key = split[0].split(" ");
            String decade = split_key[0];
            String w1 = split_key[1];
            String w2 = split_key[2];
            context.write(new ComparableKey(decade, w1, w2, npmi), new Text(npmi));
        }

    }

    public static class ReducerClass extends Reducer<ComparableKey,Text,Text,Text> {

        @Override
        public void reduce(ComparableKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.getDecade() + " " + key.getW1() + " " + key.getW2()), new Text(key.getNpmi()));
        }

    }

    public static class PartitionerClass extends Partitioner<ComparableKey, Text> {
        @Override
        public int getPartition(ComparableKey key, Text value, int numPartitions) {
            String  decade = key.getDecade().toString() + "GalKeter";//partition by decade, "GalKeter" added for better distribution of the keys by decade
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 6 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step6");
        job.setJarByClass(step6.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(ComparableKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://keterone1/output_step5"));
        FileOutputFormat.setOutputPath(job, new Path("s3://keterone1/output_step6"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}