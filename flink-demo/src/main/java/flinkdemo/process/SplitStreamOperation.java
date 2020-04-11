package flinkdemo.process;

import flinkdemo.common.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// This class implements split and merge streams operations.
public class SplitStreamOperation implements ProcessStream {

    // This method receives a stream and executes split and merge operations
    public void process(DataStream<String> inputStream) {

            // Define a separate stream for Players
            final OutputTag<Tuple2<String,String>> playerTag
                    = new OutputTag<Tuple2<String,String>>("player"){};

            // Define a separate stream for Singers
            final OutputTag<Tuple2<String,Integer>> singerTag
                    = new OutputTag<Tuple2<String,Integer>>("singer"){};

            // Convert each record to an InputData object and split the main stream into two side streams.
            SingleOutputStreamOperator<InputData> inputDataMain
                    = inputStream
                    .process(new ProcessFunction<String, InputData>() {

                        @Override
                        public void processElement(
                                String inputStr,
                                Context ctx,
                                Collector<InputData> collInputData) {

                            Utils.print(Utils.COLOR_CYAN, "Received record : " + inputStr);

                            // Convert a String to an InputData Object
                            InputData inputData = InputData.getDataObject(inputStr);

                            switch (inputData.getType())
                            {
                                case "Singer":

                                    //Create output tuple with name and count
                                    ctx.output(singerTag,
                                            new Tuple2<String,Integer>
                                                    (inputData.getName(), inputData.getScore()));
                                    break;
                                case "Player":
                                    // Create output tuple with name and type;
                                    // if the newly created tuple doesn't match the playerTag type then a compilation error is raised ("method output cannot be applied to given types")
                                    ctx.output(playerTag,
                                            new Tuple2<String, String>
                                                    (inputData.getName(), inputData.getType()));
                                    break;
                                default:
                                    // Collect main output  as InputData objects
                                    collInputData.collect(inputData);
                                    break;
                            }
                        }
                    });

            // Collects the side output stream
            DataStream<Tuple2<String,String>> playerTrail
                    = inputDataMain.getSideOutput(playerTag);

            // Collects the side output stream
            DataStream<Tuple2<String,Integer>> singerTrail
                    = inputDataMain.getSideOutput(singerTag);

            Integer windowInterval = 5;

            // Print the elements in the Player stream.
            // In this example, need to specify the return type of map method to avoid a runtime error (org.apache.flink.api.common.functions.InvalidTypesException): "the return type of function could not be determined automatically"
            // Instead of using returns method, the alternative is to implement the 'ResultTypeQueryable' interface.
            playerTrail.
                    map((MapFunction<Tuple2<String, String>,Tuple2<String, String>>) elem->
                    {
                                Utils.print(Utils.COLOR_BLUE, String.format("in Player side stream. Processing player name: %s, type %s", elem.f0, elem.f1));
                                return elem;
                    }).
                    returns(Types.TUPLE(Types.STRING, Types.STRING));

            //Print the count of all the events in the original stream
            PrintStream.generateDataStreamCount(
                inputStream.map( i -> (Object)i),
                windowInterval,
                String.format("All entities in last %d secs", windowInterval));

            // Print Singers count
            PrintStream.generateDataStreamCount(
                    singerTrail.map( i -> (Object)i),
                    windowInterval, String.format("Singer entities in last %d secs", windowInterval));

            // Print Players count
            PrintStream.generateDataStreamCount(
                    playerTrail.map( i -> (Object)i),
                    windowInterval,
                    String.format("Player entities in last %d secs", windowInterval));

            // Print the count of elements that were left in the main stream after the split (after splitting the Player and Singer entities)
            PrintStream.generateDataStreamCount(
                inputDataMain.map( i -> (Object)i),
                windowInterval,
                String.format("Other entities  in last %d secs", windowInterval));

            // After splitting the streams, let's combine them into a new stream
            DataStream<Tuple4<String, String, String, Integer>>  mergedStream = mergeStreams(singerTrail, playerTrail);

            // Print the merged data stream
            mergedStream
                    .map(new MapFunction<Tuple4<String, String, String, Integer>,
                        Tuple4<String, String, String, Integer>>() {
                    @Override
                    public Tuple4<String, String, String, Integer>
                    map(Tuple4<String, String, String, Integer> user) {
                        Utils.print(Utils.COLOR_YELLOW, "A merged record: " + user);
                        return null;
                    }
                });
    }

    // This method implements a merge stream operation. The inputs are two types of streams, which converges into a third type.
    public static DataStream<Tuple4<String, String, String, Integer>>
    mergeStreams(DataStream<Tuple2<String, Integer>> singerStream, DataStream<Tuple2<String, String>> playerStream) {

        // The returned stream definition includes both streams data type
        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, String>> mergedStream
                = singerStream
                .connect(playerStream);


        DataStream<Tuple4<String, String, String, Integer>> combinedStream
                = mergedStream.map(new CoMapFunction<
                Tuple2<String, Integer>, //Stream 1
                Tuple2<String, String>, //Stream 2
                Tuple4<String, String, String, Integer> //Output
                >() {

            @Override
            public Tuple4<String, String, String, Integer>  //Process Stream 1
            map1(Tuple2<String, Integer> singer) throws Exception {
                return new Tuple4<String, String, String, Integer>
                        ("Source: singer stream", singer.f0, "", singer.f1);
            }

            @Override
            public Tuple4<String, String, String, Integer> //Process Stream 2
            map2(Tuple2<String, String> player) throws Exception {
                return new Tuple4<String, String, String, Integer>
                        ("Source: player stream", player.f0, player.f1, 0);
            }
        });

        return  combinedStream;
    }
}
