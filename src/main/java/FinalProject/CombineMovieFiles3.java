package FinalProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/** @noinspection Duplicates */
public class CombineMovieFiles3 {

    public static class CombineMovieMapper extends Mapper<Object, Text, Text, Text>{
        Map<Text, Text> titleMap;
        Text mapKey = null;

        public void setup(Context context) throws IOException, InterruptedException{
            titleMap = new HashMap<Text, Text>();
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0){

                URI titleFileUri = context.getCacheFiles()[0];
                if (titleFileUri != null) {
                    File titles = new File("./movie_titles.txt");
                    try {
                        BufferedReader reader = new BufferedReader(new FileReader(titles));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String lineArray[] = line.split(",");
                            Text key = new Text(lineArray[0]);
                            Text value = new Text(lineArray[0] + "," + lineArray[1] + "," + lineArray[2]);
                            titleMap.put(key, value);
                        }
                    } catch (IOException x) {
                        System.err.format("IOException: %s%n", x);
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String current;
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text title;
            Text output;

            while (itr.hasMoreTokens()) {
                current = itr.nextToken();
                String[] line = current.split(",");
                if (line.length == 1){
                    mapKey = new Text(current.substring(0, current.length()-1));
                } else {
                    title = titleMap.get(mapKey);
                    output = new Text(title.toString() + "," + current);
                    context.write(null, output);
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

        job.addCacheFile(new URI(s3File + "/movie_titles.txt"));

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
