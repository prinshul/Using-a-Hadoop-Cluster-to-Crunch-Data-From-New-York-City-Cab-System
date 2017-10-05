package q1;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Passenger_Trip_Avg_Mapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	String days[]={"Mon","Tues","Wed","Thurs","Fri","Sat","Sun"};  //create days
	// create month array
	String months[]={"Jan","Feb","Mar","Apr","May","June","July","Aug","Sep","Oct","Nov","Dec"};
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		word.set(value);
		String nextLine =word.toString();

		String [] cols=nextLine.split(",");

		// check if the data is not missing
		if(cols.length==18 && !cols[0].equals("vendor_id")) //ignore headers
		{
			// different key like total passengers, total trips, total passengers per day
			// total trips per day, total trips, total avg passengers are created
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {
				Date date=df.parse(cols[1]);   // get the data in the form of hours interval
				Text monthkey=new Text(months[date.getMonth()]+"_Total_Passengers"); //  total passengers key
				context.write(monthkey, new IntWritable(Integer.parseInt(cols[3])));
				context.write(new Text(months[date.getMonth()]+"_Total_Trips"), new IntWritable(1));
                 
				Text daykey=new Text(days[date.getDay()]+"_PerDay_Total_Passengers");
				context.write(daykey, new IntWritable(Integer.parseInt(cols[3])));
				context.write(new Text(days[date.getDay()]+"_PerDay_Total_Trips"), new IntWritable(1));

				context.write(new Text("Total_Trips"), new IntWritable(1));
				context.write(new Text("Total_Avg_Passenger"), new IntWritable(Integer.parseInt(cols[3])));
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}


	}
}