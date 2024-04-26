import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class step5 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] split = line.toString().split("\t");//forward all
            String key = split[0];
            String value = split[1];
            context.write(new Text(key), new Text(value));
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        private double decade_npmi_sum;
        private double minPmi;
        private double relMinPmi;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            minPmi = context.getConfiguration().getDouble("minPmi" , -1);
            relMinPmi = context.getConfiguration().getDouble("relMinPmi" , -1);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] split_key = key.toString().split(" ");
            if (split_key[1].equals("*")) { //(d *)
                double sum = 0;
                for(Text value : values){
                    sum += Double.parseDouble(value.toString());
                }
                decade_npmi_sum = sum;
            }
            else { //calculate relative npmi for (d w1 w2) keys
                for (Text value : values) { //should be only one value
                    double npmi = Double.parseDouble(value.toString());
                    double rel_npmi = npmi / decade_npmi_sum;
                    if (npmi >= minPmi || rel_npmi >= relMinPmi){
                        context.write(key, new Text(npmi + ""));
                    }
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String [] split_key = key.toString().split(" ");
            String decade = split_key[0]+ "GalKeter"; //partition by decade, "GalKeter" added for better distribution of the keys by decade
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
    
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] split_key = key.toString().split(" ");
            if (split_key[1].equals("*")) { //(d *)
                double sum = 0;
                for(Text value : values){
                    sum += Double.parseDouble(value.toString());
                }
                context.write(key, new Text(sum + ""));
            }
            else { //cant calculate relative npmi for (d w1 w2) keys -> forward
                for (Text value : values) { //should be only one value
                    context.write(key, value);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        conf.setDouble("minPmi", Double.parseDouble(args[1]));
        conf.setDouble("relMinPmi", Double.parseDouble(args[2]));
        Job job = Job.getInstance(conf, "step5");
        job.setJarByClass(step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("s3://keterone1/output_step4"));
        FileOutputFormat.setOutputPath(job, new Path("s3://keterone1/output_step5"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}