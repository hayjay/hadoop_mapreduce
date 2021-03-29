package dev.nurudeen;


/**
 What does the code below does ?
    The codes below are java libraries and dependencies.
    The dependencies and libraries below are what hadoop uses to run and perform some major tasks.
    The code below also includes the dependencies java uses to execute its built in function, classes and methods.
*/
import java libraries and dependencies
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 What does the code below does ?
    The code below is the main and the entry point of all java applications

    The main function is what gets executed before all other programs in java executes when its been run.
*/
public static void main(String[] args) throws Exception {
    // create the Job
    JobConf jobConfiguration = new JobConf(Conference.class);
    //defines and set the job name
    jobConfiguration.setJobName("NumberOfConferenceOcurrence");

    jobConfiguration.setOutputKeyClass(Text.class);
    jobConfiguration.setOutputValueClass(IntWritable.class);

    jobConfiguration.setMapperClass(ConferenceMapper.class);
    jobConfiguration.setCombinerClass(ConferenceReducer.class);
    jobConfiguration.setReducerClass(ConferenceReducer.class);

    jobConfiguration.setInputFormat(TextInputFormat.class);
    jobConfiguration.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(jobConfiguration, new Path("input_file"));
    FileOutputFormat.setOutputPath(jobConfiguration, new Path("output_file"));

    //execute the code in the mapreduce queuing system using the .runJob() method and this method takes in the jobConfuguration that is been set
    JobClient.runJob(jobConfiguration);
}

// Here is the class name that hadoop would use in computing the generated conference numbers.
// The class name is : BigDataCalculateEachConferenceNumbersInPapers
public class BigDataCalculateEachConferenceNumbersInPapers {

    /**
        What does the code below does ?
        in order to use some of the Map Reduce built in methods and classes.
        This ConferenceMapper class extends or inherits from the MapReduceBase class using the extends key word
        And then it implements the Mapper using the default implements keyword in java
    */
    static class ConferenceMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable longWritable,
                        Text map_text,
                        OutputCollector<Text, IntWritable> emmitter,
                        Reporter reporter) throws IOException {
            String textFromPaper = file_text.toString(); //converts file texts to string using the builtin java toString() method
            // split the text fetched from paper using regular expression on the built in java split method("\\|")
            // In order to fetch the conference word from the given string, it is obvious that the pipe (|) seprates each word so we split each word in the text using | pipe
            String[] splitEachWord = textFromPaper.split("\\|");

            conference_word = this.getConferenceName(splitEachWord);
            //emits and executes each conference number
            emmitter.collect(new Text(conference_word), new IntWritable(1));
        }
    }

    // this self defined function is used to get the conference Name and it is reusable in other places if needed
    static class getConferenceName(Text paper_text) {
        // makes use of 2 since the conference word occurs in the second index of the array.
        // I am able to detect and study this based on the patten and position at which the conference is placed
        return String getConferenceName = paper_text[2];
    }

     /**
        What does the code below does ?
        in order to use some of the Map Reduce built in methods and classes.
        This ConferenceReducer class also extends or inherits from the MapReduceBase class using the extends key word
        And then it implements the Mapper using the default implements keyword in java
    */
    static class ConferenceReducer extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key,
                           Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> emmitter,
                           Reporter reporter) throws IOException {
            /**
                In order to reduce and conquer what the mapper has executed using the emmiter and to count the number of times the conference occurs,
                we initialize a counter below using number_of_occurence = 0
            */
            int number_of_occurence = 0;
            // Here a while loop is performed on every single values the mapper produce 
            // And then the .hasNext method is been called on every single output which checks if the conference exist twice
            while (values.hasNext()) {
                IntWritable next = values.next();
                int i = next.get();
                number_of_occurence = number_of_occurence + i;
            }
            // finally the emitter or the outputter returnes the key and the number of times the conference occurs
            // the emitter/OutputCollector class calls the collect method and then takes in the key and the number_of_occurence variable 
            // and it finally produces then number of times the conference occurs by simply incrementing the counter
            emmitter.collect(key, new IntWritable(number_of_occurence));
        }
    }
}
