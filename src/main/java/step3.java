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

public class step3 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] split = line.toString().split("\t");
            String key = split[0];
            String value = split[1];
            String [] split_key = key.split(" ");
            if(split_key[2].equals("*")){//(d * *) -> forward
                context.write(new Text(key), new Text(value));
            }
            else{
                String decade = split_key[0];
                String w1 = split_key[1];//note : w1 can be *
                String w2 = split_key[2];
                if(w1.equals("*")){ //(d * w2)
                    context.write(new Text(decade + " " + w2 + " " + "1"), new Text(value));// (d w2 1 , c(w2))
                }
                else{ //(d w1 w2)
                    context.write(new Text(decade + " " + w2 + " " + "2"), new Text(w1 + " " + w2 + " " + value));// (d w2 2 , w1 w2 c(w1,w2) c(w1))
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        private long curr_Cw2;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String [] split_key = key.toString().split(" ");
            if(split_key[2].equals("*")){//(d * *) -> forward
                for(Text value : values){
                    context.write(key, value);
                }
            }
            else if (split_key[2].equals("1")) { //(d w2 1 , c(w2))
                curr_Cw2 = Long.parseLong(values.iterator().next().toString()); //assuming only one value exists
            }
            else { // (d w2 2 , w1 w2 c(w1,w2) c(w1))
                for (Text value : values) {
                    String[] split_val = value.toString().split(" ");
                    String newKey = split_key[0] + " " + split_val[0] + " " + split_val[1]; // (d w1 w2)
                    String newValue = split_val[2] + " " + split_val[3] + " " + curr_Cw2; //(c(w1,w2) c(w1) c(w2))
                    context.write(new Text(newKey), new Text(newValue));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String [] split_key = key.toString().split(" ");
            String partitionBy = split_key[0] + split_key[1]; //partition by (d w2)
            return (partitionBy.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step3");
        job.setJarByClass(step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("s3://keterone1/output_step2"));
        FileOutputFormat.setOutputPath(job, new Path("s3://keterone1/output_step3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}