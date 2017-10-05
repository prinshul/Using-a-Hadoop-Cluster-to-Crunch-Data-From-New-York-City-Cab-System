package q2;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TripDistance_Average_Mapper extends Mapper<Object, Text, Text, FloatWritable> {

	private Text word = new Text();
	String days[]={"Mon","Tues","Wed","Thurs","Fri","Sat","Sun"};
	String months[]={"Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec"};

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		word.set(value);
		String nextLine =word.toString();

		String [] columns=nextLine.split(",");


		if(columns.length==18 && !columns[0].equals("vendor_id"))
		{
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {

				Date date=df.parse(columns[1]);
				Text monthkey=new Text(months[date.getMonth()]+"_Total_Distance");
				context.write(monthkey, new FloatWritable(Float.parseFloat(columns[4])));
				context.write(new Text(months[date.getMonth()]+"_Total_Trips"), new FloatWritable(1));

				Text daykey=new Text(days[date.getDay()]+"_PerDay_Total_Distance");
				context.write(daykey, new FloatWritable(Float.parseFloat(columns[4])));
				context.write(new Text(days[date.getDay()]+"_Day_Total_Trip_Number"), new FloatWritable(1));

				context.write(new Text("Total_Trip_Number"), new FloatWritable(1));
				context.write(new Text("Total_Distance"), new FloatWritable(Float.parseFloat(columns[4])));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}


	}
}