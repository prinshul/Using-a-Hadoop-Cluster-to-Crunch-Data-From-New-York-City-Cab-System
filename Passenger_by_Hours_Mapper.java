package q4;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Passenger_by_Hours_Mapper extends Mapper<Object, Text, Text, FloatWritable> {
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
               // time duration, total trips, total duration, total trip of passengers, total number
				// of trips, time duration are the keys
				Text columnkey=new Text("Time_Duration_"+date.getHours()+"-"+(date.getHours()+1)+"_TripPassangers");
				context.write(columnkey, new FloatWritable(Float.parseFloat(cols[3])));
				context.write(new Text("Total Trips of Passangers"), new FloatWritable(Float.parseFloat(cols[3])));
				context.write(new Text("Time_Duration_"+date.getHours()+"-"+(date.getHours()+1)+"_Total_Trips"), new FloatWritable(1));
				context.write(new Text("Total Number of Trips"), new FloatWritable(1));
			}		
			catch (ParseException e) {
				e.printStackTrace();
			}

		}


	}

}