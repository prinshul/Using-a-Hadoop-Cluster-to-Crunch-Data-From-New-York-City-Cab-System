package q4_version;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Passenger_perHour_by_Day_Combiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
 private FloatWritable result = new FloatWritable();
 public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws
IOException, InterruptedException {
 float sum = 0;
 for (FloatWritable val : values) {
 sum += val.get();
 }
 result.set(sum);
 context.write(key, result);
 }
}