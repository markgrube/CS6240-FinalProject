package FinalProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/** @noinspection Duplicates */
public class CombineMovieFiles2 {

    public static class CombineMovieMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String current;
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text mapKey = null;

            while (itr.hasMoreTokens()) {
                current = itr.nextToken();
                String[] line = current.split(",");
                if (line.length == 1){
                    mapKey = new Text(current.substring(0, current.length()-1));
                } else {
                    String output = line[0] + "," + mapKey + "," + line[1];
                    context.write(null, new Text(output));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "finalproject");
        job.setJarByClass(CombineMovieFiles.class);
        job.setNumReduceTasks(0);

        String s3File = args[0];

        //Set Mapper Class
        job.setMapperClass(CombineMovieMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
