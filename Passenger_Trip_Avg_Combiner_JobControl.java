package q1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Passenger_Trip_Avg_Combiner_JobControl {
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = new Job();
 job.setJarByClass(Passenger_Trip_Avg_Combiner_JobControl.class);
 job.setMapperClass(Passenger_Trip_Avg_Mapper.class);  //set the mapper class
 job.setCombinerClass(Passenger_Trip_Avg_Combiner.class); //set combiner class
 job.setReducerClass(Passenger_Trip_Avg_Reducer.class);  //set reducr class
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(IntWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));  //input path
 FileOutputFormat.setOutputPath(job, new Path(args[1])); //output path
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}