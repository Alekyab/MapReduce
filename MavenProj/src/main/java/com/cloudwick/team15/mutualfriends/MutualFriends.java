package com.cloudwick.team15.mutualfriends;
import java.io.BufferedWriter;
import java.io.File;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MutualFriends extends Configured implements Tool {

    @SuppressWarnings("unchecked")
	public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf(
                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
                    .getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Job job = new Job(getConf());
        job.setJarByClass(MutualFriends.class);
        job.setJobName(this.getClass().getName());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(Reducerclass.class);
        job.setSortComparatorClass(Comp.class);
        job.setPartitionerClass(CPartitioner.class);
        job.setGroupingComparatorClass(GroupingComp.class);
        
        job.setMapOutputKeyClass(CustomInput.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static boolean deleteDir(File dir) 
    { 
      if (dir.isDirectory()) 
    { 
      String[] children = dir.list(); 
      for (int i=0; i<children.length; i++)
      { 
        boolean success = deleteDir(new File(dir, children[i])); 
        if (!success) 
        {  
          return false; 
        } 
      } 
    }  
      // The directory is now empty or this is a file so delete it 
      return dir.delete(); 
    }
    public static void main(String[] args) throws Exception {
    
    	File output  = new File(args[1]);
    	deleteDir(output);
    	int exitCode = ToolRunner.run(new MutualFriends(), args);
        System.exit(exitCode);
    }
}
