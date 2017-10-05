package q4_version;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Passenger_perHour_by_Day_Mapper extends Mapper<Object, Text, Text, FloatWritable> {
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
                 // create separate key for weekend and week day
				Date date=df.parse(cols[1]);
				if(date.getDay()==0 || date.getDay()==6)  //for the weekend
				{
					Text columnkey=new Text("Week_End"+"_Time_Duration_"+date.getHours()+"-"+(date.getHours()+1)+"_Trip_Passangers");
					context.write(columnkey, new FloatWritable(Float.parseFloat(cols[3])));
					context.write(new Text("Weekend Trip of Passangers"), new FloatWritable(Float.parseFloat(cols[3])));
					context.write(new Text("Weekend Trip Total"), new FloatWritable(1));
					context.write(new Text("Weekend"+"_TimeDuration_"+date.getHours()+"-"+(date.getHours()+1)+"_Total_Trips"), new FloatWritable(1));

				}
				else    //week days
				{	
					Text columnkey=new Text("Workingday"+"_TimeDuration_"+date.getHours()+"-"+(date.getHours()+1)+"_Trip_Passangers");
					context.write(columnkey, new FloatWritable(Float.parseFloat(cols[3])));
					context.write(new Text("Workingday Trip for Passangers"), new FloatWritable(Float.parseFloat(cols[3])));
					context.write(new Text("Workingday Trip Count"), new FloatWritable(1));
					context.write(new Text("Workingday"+"_TimeDuration_"+date.getHours()+"-"+(date.getHours()+1)+"_Total_Trip_Count"), new FloatWritable(1));
				}
				context.write(new Text("Total number of Trips"), new FloatWritable(1));
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}


	}

}