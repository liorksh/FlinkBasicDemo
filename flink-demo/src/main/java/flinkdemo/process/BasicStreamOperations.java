package flinkdemo.process;

import flinkdemo.common.*;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.io.IOException;

import org.apache.flink.core.fs.Path;

/*
This program reads a generate a files stream, computes a Map and Reduce operation,
and writes the output to a file
 */
public class BasicStreamOperations implements ProcessStream {

    public void process(DataStream<String> dataStream) throws IOException {

        // Convert each record to an InputData object; each new line is considered a new record
        DataStream<InputData> inputDataObjectStream
                = dataStream
                .map((MapFunction<String, InputData>) inputStr -> {
                    System.out.println("--- Received Record : " + inputStr);
                    return InputData.getDataObject(inputStr);
                });


        Integer windowInterval = 5;

        // Print every item in the stream
        PrintStream.printObject(
                inputDataObjectStream.map(obj -> (InputData) obj), windowInterval);


        //Print the number of items in the stream
        PrintStream.generateDataStreamCount(
                inputDataObjectStream.map(i -> (InputData) i),
                windowInterval,
                "InputData objects count in the last " + windowInterval + " seconds");


        /************************* Group By Key implementation *****************/

        // Convert each record to a Tuple with name and score
        DataStream<Tuple2<String, Integer>> userCounts
                = inputDataObjectStream
                .map(new MapFunction<InputData,Tuple2<String,Integer>>() {

                    @Override
                    public Tuple2<String,Integer> map(InputData item) {
                        return new Tuple2<String,Integer>(item.getName() ,item.getScore() );
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)  // returns KeyedStream<T, Tuple> based on the first item ('name' fields)
                //.timeWindowAll(Time.seconds(windowInterval)) // DO NOT use timeWindowAll for a key-based stream
                .timeWindow(Time.seconds(2)) // return WindowedStream<T, KEY, TimeWindow>
                //.countWindow(5) // reaching to 5 items in the group and then run the reduce method
                .reduce((x,y) -> new Tuple2<String,Integer>( x.f0+"-"+y.f0, x.f1+y.f1));

        // Print User name and sum of score.
        userCounts.print();


        /************************* Sink implementation *****************/

        // Prepare the output directory (sink). It will store the output of the process action on the incoming stream.
        String outputDir = "data/sink_summary";

        Utils.ensureFolderExists(outputDir);

        // https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/streamfile_sink.html

        // Define a time window and count the number of records
        DataStream<Tuple2<String, Integer>> inputCountSummary
                = inputDataObjectStream
                .map(item
                        -> new Tuple2<String, Integer>
                        (String.valueOf(System.currentTimeMillis()), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .timeWindowAll(Time.seconds(windowInterval))
                .reduce((x, y) ->
                        (new Tuple2<String, Integer>(x.f0, x.f1 + y.f1)));

        //Setup a streaming file sink to the output directory
        final StreamingFileSink<Tuple2<String, Integer>> countSink
                = StreamingFileSink
                .forRowFormat(new Path(outputDir),
                        new SimpleStringEncoder<Tuple2<String, Integer>>
                                ("UTF-8"))
                .build();

        // Add the sink file stream to the DataStream; with that, the inputCountSummary will be written into the FileSink path
        inputCountSummary.addSink(countSink);
    }
}
