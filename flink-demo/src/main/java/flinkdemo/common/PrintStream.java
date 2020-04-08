package flinkdemo.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PrintStream {

    public static void printObject(DataStream<InputData> dataStreamInput, Integer windowInterval){

        dataStreamInput.filter(elem ->
                        elem.getName().length()>3
                )
                .map(elem -> {
                      Utils.print(Utils.COLOR_YELLOW, "Print item : " + elem);
                        return (elem);

                }).timeWindowAll(Time.seconds(windowInterval));
    }

    public static void generateDataStreamCount(DataStream<Object> objectDataStream, Integer windowTime, String msgToPrint) {

        SingleOutputStreamOperator<Integer> map = objectDataStream
                // Define a counter record for each input item
                .map(item
                        -> new Integer(1))
                .returns(Types.INT)

                // Set window by time
                .timeWindowAll(Time.seconds(windowTime))

                // Accumulate the number of records in each time window's interval
                .sum(0)

                // Execute print method, since there is only one item in the stream the method will be executed once
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer recordCount) throws Exception {
                        Utils.print(Utils.COLOR_GREEN, msgToPrint + " : " + recordCount);
                        return recordCount;
                    }
                });
    }

    public static void generateDataStreamCount_UsingReduce(DataStream<Object> objectDataStream, Integer windowTime, String msgToPrint) {

        SingleOutputStreamOperator<Integer> map = objectDataStream
                // Define a counter record for each input item
                .map(item
                        -> new Integer(1))
                // The returns statement specifies the produced type is Integer
                .returns(Types.INT)

                // Set window by time
                .timeWindowAll(Time.seconds(windowTime))

                // Accumulate the number of records in each time window's interval. The same can be achieved by calling 'sum(0)' method.
                .reduce((x, y) ->
                        (new Integer(x + y)))

                // Execute print method, since there is only one item in the stream the method will be executed once
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer recordCount) throws Exception {
                        System.out.println(msgToPrint + " : " + recordCount);
                        return recordCount;
                    }
                });
    }

    public static void generateDataStreamCount_UsingReduceFixedMessage(DataStream<Object> objectDataStream, Integer windowTime, String msgToPrint) {

        SingleOutputStreamOperator<Integer> map = objectDataStream
                // Define a counter record for each input item
                .map(item
                        -> new Tuple2<String, Integer>
                        (msgToPrint, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))

                // Set window by time
                .timeWindowAll(Time.seconds(windowTime))

                // Accumulate the number of records in each time window's interval
                .reduce((x, y) ->
                        (new Tuple2<String, Integer>(x.f0, x.f1 + y.f1)))

                // Execute print method, since there is only one item in the stream the method will be executed once
                .map(new MapFunction<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public Integer map(Tuple2<String, Integer> recordCount) throws Exception {
                        System.out.println(recordCount.f0 + " : " + recordCount.f1);
                        return recordCount.f1;
                    }
                });
    }
}
