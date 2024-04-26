import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private static final HashSet<String> StopWords = new HashSet<> (Arrays.asList("about","above","across","after","afterwards","again","against","all","almost","alone","along","already","also","although","always",
        "am","among","amongst","amoungst","amount","an","and","another","any","anyhow","anyone","anything","anyway","anywhere","are","around","as","at",
        "back","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","below","beside","besides","between","beyond","bill","both","bottom","but","by","call","can","cannot","cant","co","computer","con","could","couldnt","cry",
        "de","describe","detail","do","done","down","due","during","each","eg","eight","either","eleven","else","elsewhere","empty","enough","etc","even","ever","every","everyone","everything","everywhere","except","few","fifteen","fify","fill","find",
        "fire","first","five","for","former","formerly","forty","found","four","from","front","full","further","get","give","go","had","has","hasnt","have","he",
        "hence","her","here","hereafter","hereby","herein","hereupon","hers","herself","him","himself","his","how","however","hundred","i","ie","if","in","inc","indeed","interest","into","is","it","its","itself","keep","last","latter","latterly","least","less","ltd","made","many","may","me","meanwhile","might","mill","mine","more","moreover","most","mostly","move","much","must","my","myself","name","namely","neither","never","nevertheless","next","nine","no","nobody","none","noone","nor","not","nothing","now","nowhere","of","off","often","on",
        "once","one","only","onto","or","other","others","otherwise","our","ours","ourselves","out","over","own","part","per","perhaps","please","put","rather","re","same","see","seem","seemed","seeming","seems","serious","several","she","should","show","side","since","sincere","six","sixty","so","some","somehow","someone","something","sometime","sometimes","somewhere","still","such","system","take","ten","than","that","the","their","them","themselves","then","thence","there","thereafter","thereby","therefore","therein","thereupon","these","they","thick","thin","third","this","those","though","three","through","throughout","thru","thus","to","together","too","top","toward","towards","twelve","twenty","two","un","under","until","up","upon","us","very","via","was","we",
        "well","were","what","whatever","when","whence","whenever","where","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","whoever","whole","whom","whose","why","will","with","within","without","would","yet","you","your","yours","yourself","yourselves")) ;

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException,  InterruptedException {
            String [] split = line.toString().split("\t");
            if (split.length >= 3) {
                String [] bi_gram = split[0].split(" ");
                if(bi_gram.length>=2){
                    double chance = Math.random();
                    if(chance<0.5){
                        if(!(StopWords.contains(bi_gram[0]) || StopWords.contains(bi_gram[1]))){
                            String decade = decadeConvert(split[1]);
                            String w1 = bi_gram[0];
                            String w2 = bi_gram[1];
                            String occurrences = split[2];
                            context.write(new Text(decade + " " + w1 + " " + w2), new Text(occurrences));//(decade w1 w2 , occurrences)
                            context.write(new Text(decade + " " + w1 + " " + "*"), new Text(occurrences));//(decade w1 * , occurrences)
                            context.write(new Text(decade + " " + "*" + " " + w2), new Text(occurrences));//(decade * w2 , occurrences)
                            context.write(new Text(decade + " " + "*" + " " + "*"), new Text(occurrences));//(decade * * , occurrences)
                        }
                    }
                }
            }
        }

        public String decadeConvert(String year){
            return year.substring(0, 3) + "0";
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (Text value : values) {
                sum += Long.parseLong(value.toString());
            }
            context.write(key, new Text(""+sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        //long MaxSplitSize = 16000000;
        //conf.setLong("mapreduce.input.fileinputformat.split.maxsize", MaxSplitSize);
        Job job = Job.getInstance(conf, "step1");
        job.setJarByClass(step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"));
        FileOutputFormat.setOutputPath(job, new Path("s3://keterone1/output_step1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}