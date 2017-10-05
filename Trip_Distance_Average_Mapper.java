package q5;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Trip_Distance_Average_Mapper extends Mapper<Object, Text, Text, FloatWritable> {
	private Text word = new Text();
	public void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {
		word.set(value);
		String nextLine =word.toString();

		String [] cols=nextLine.split(",");

		if(cols.length==18 && !cols[0].equals("vendor_id"))
		{

			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {

				Date date=df.parse(cols[1]);

				Text columnkey=new Text("TimeDuration_"+date.getHours()+"-"+(date.getHours()+1)+"_Trip_Distance");
				context.write(columnkey, new FloatWritable(Float.parseFloat(cols[4])));
				context.write(new Text("Trip_Distance"), new FloatWritable(Float.parseFloat(cols[4])));
				context.write(new Text("Time_Duration_"+date.getHours()+"-"+(date.getHours()+1)+"_Total_Trip_Count"), new FloatWritable(1));
				context.write(new Text("Total trip count"), new FloatWritable(1));
			}		
			catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}


	}

}