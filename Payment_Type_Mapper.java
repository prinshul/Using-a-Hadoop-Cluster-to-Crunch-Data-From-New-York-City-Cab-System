package q3;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Payment_Type_Mapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	public void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {

		word.set(value);
		String nextLine =word.toString();

		String [] columns=nextLine.split(",");


		if(columns.length==18 && !columns[0].equals("vendor_id"))
		{

			context.write(new Text(columns[11]), new IntWritable(1));
		}

	}
}