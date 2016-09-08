    import java.io.IOException;
    import java.util.*;
    import java.util.Scanner;
    import java.util.ArrayList;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.conf.*;
    import org.apache.hadoop.io.*;
    import org.apache.hadoop.mapreduce.*;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

    public class Movie_Recommendation {
    	public static String key_movie;
    	public static int j = 0;
    	public static Vector<String> list = new Vector<String>();
    	
    	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {		//Map class to Mapping Interesting 							object
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {//Map Method
            String whole_line = value.toString();//Access Line 						from file name given	
            list.add(whole_line);	//add to list vector
            String[] whole_linearray = whole_line.split(", ");
        
            int i=0;
            
            while(whole_linearray.length>i)//run until length of file
            {
            	if (whole_linearray[i].toUpperCase().equals(key_movie.toUpperCase()))//check for entered movie name with name given in file
            	{
            		word.set(whole_linearray[i-1]);
            		context.write(word,new IntWritable(-(i-1)));
            	}
            	i+=1;
            }
        }
     } 

     public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> { //reduce class to sort and finslize output

        public void reduce(Text key, Iterable<IntWritable> values, Context context) //reduce method
          throws IOException, InterruptedException {
            String out = null;
            
            int i=0;
            while(i < list.size()) //read values from file
            {
            	String[] whole_line = list.get(i).split(", ");
            	while( whole_line.length - 1>j)
            	{
            		if(whole_line[j].equals(key.toString()))
            		{
            			if(!(whole_line[j+1].equals(key_movie)))
            			{
	            			if (out == null)
	            			{
	            				out =  whole_line[j+1];
	            			}
	            			else
	            			{
	            				out = out+", "+whole_line[j+1];
	            			}
	            		}
            			j++;
            		}
            	}	
            	i+=1;
            }
            Text word = new Text();
            word.set("People who watched "+key_movie+" also watched "+out);
            context.write(word,new IntWritable(1));
       
        }
     }

     public static void main(String[] args) throws Exception {
    
    	 System.out.println("Enter a movie name:");
    	 Scanner input = new Scanner(System.in);
    	 key_movie = input.nextLine();
        Configuration conf = new Configuration();

            Job job = new Job(conf, "test");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
     }

    }