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

public class step4 {

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
        private long curr_N;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] split_key = key.toString().split(" ");
            if (split_key[1].equals("*") && split_key[2].equals("*")) { //(d * *)
                curr_N = Long.parseLong(values.iterator().next().toString()); //assuming only one value exists
            }
            else { //calculate npmi
                for (Text value : values) { //should be only one value
                    String[] split_val = value.toString().split(" ");
                    long Cw1w2 = Long.parseLong(split_val[0]);
                    long Cw1 = Long.parseLong(split_val[1]);
                    long Cw2 = Long.parseLong(split_val[2]);
                    double npmi = calcNpmi(Cw1w2, Cw1, Cw2);
                    context.write(key, new Text("" + npmi)); //(d w1 w2 , npmi(w1,w2))
                    //in order to calculate relative npmi in next step:
                    context.write(new Text(split_key[0] + " " + "*"),new Text("" + npmi) ); //(d * , npmi(w1,w2))
                }
            }
        }

        private double calcNpmi(long Cw1w2, long Cw1, long Cw2){
            double pmi = Math.log(Cw1w2) + Math.log(curr_N) - Math.log(Cw1) - Math.log(Cw2);
            double Pw1w2 = (double) Cw1w2 / curr_N;
            return pmi / (-1 * Math.log(Pw1w2));
            }
        }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String [] split_key = key.toString().split(" ");
            String decade = split_key[0] + "GalKeter"; //partition by decade, "GalKeter" added for better distribution of the keys by decade
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step4");
        job.setJarByClass(step4.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("s3://keterone1/output_step3"));
        FileOutputFormat.setOutputPath(job, new Path("s3://keterone1/output_step4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}