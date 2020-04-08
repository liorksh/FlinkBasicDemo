package flinkdemo.process;

import flinkdemo.common.FilesStreamProducer;
import flinkdemo.common.Utils;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.Scanner;

/*
This program reads a generate a files stream, computes a Map and Reduce operation,
and writes the output to a file
 */
public class StreamProcessingApp {

    public static void main(String[] args) {

        try{
            // Setup a Flink streaming execution environment
            final StreamExecutionEnvironment streamEnv
                        = StreamExecutionEnvironment.getExecutionEnvironment();

            // Set the time characteristic (EventTime / ProcessingTime /IngestionTime)
            // https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_time.html
            streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

            //streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            // Keep the ordering of records, therefore use only one thread to process the messages (to avoid changing the sequence of printing)
            streamEnv.setParallelism(1);

            // The default parallel is based on the number of CPU cores
            System.out.println("\nTotal Parallel Task Slots : " + streamEnv.getParallelism() );

            // Define the data directory to which new events will be taken
            String dataDir = "data/raw_input2";

            // Ensuring the destination folder exists
            Utils.ensureFolderExists(dataDir);

            // Define the text input format based on the directory; Input Format that reads text files. Each line results in another element.
            TextInputFormat inputFormat = new TextInputFormat(new Path(dataDir));

            //Create a DataStream based on the directory
            DataStream<String> dataStream
                        = streamEnv.readFile(inputFormat,
                            dataDir,    // the origin for the events
                            FileProcessingMode.PROCESS_CONTINUOUSLY,
                            1000); // the interval to monitor the directory

            // An example for creating a DataStream object with a timestamp extractor (to support Event Time processing).
            /*
            DataStream<String> dataStream
                    = streamEnv.readFile(inputFormat,
                    dataDir,    //Director to monitor
                    FileProcessingMode.PROCESS_CONTINUOUSLY,
                    1000).
                    assignTimestampsAndWatermarks(new TimestampExtractor());
            */

            ProcessStream streamOperation = userSelection();
            while (streamOperation==null)
                streamOperation = userSelection();

            streamOperation.process(dataStream);

            //Start the File Stream generator on a separate thread
            startGeneratingStream(20, dataDir);

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming process");

        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void startGeneratingStream(Integer numOfFiles, String sourcePath){
        Utils.printMessage("Starting File Data Generator...");
        Thread genThread = new Thread(new FilesStreamProducer(numOfFiles, sourcePath));
        genThread.start();
    }

    private static ProcessStream userSelection(){
        System.out.println("Please select the stream processing function:\n" +
                "* 's' for split and merge stream operations\n" +
                "* 'b' for basic stream operation\n" +
                "* 't' for time-based window operation\n" +
                "* 'e' to exit");

        Scanner scanner = new Scanner(System.in);
        String str = scanner.next();

        // Create a stream processing object based on the user's selection.
        if(str.startsWith("s")) {
            return new SplitStreamOperation();
        }
        else if(str.startsWith("b")) {
            return new BasicStreamOperations();
        }
        else if(str.startsWith("t")) {
            return new TimeBasedWindowOperations();
        }
        else if(str.startsWith("e")) {
            System.exit(0);
        }
        else {
            System.out.println("Unknown input "+str);
        }

        return null;
    }
}
