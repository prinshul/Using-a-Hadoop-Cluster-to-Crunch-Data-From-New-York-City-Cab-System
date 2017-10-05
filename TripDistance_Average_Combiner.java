package q2;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TripDistance_Average_Combiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
 private FloatWritable result = new FloatWritable();
 public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
IOException, InterruptedException {
 int sum = 0;
 for (FloatWritable val : values) {
 sum += val.get();
 }
 result.set(sum);
 context.write(key, result);
 }
}