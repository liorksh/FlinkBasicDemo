package flinkdemo.process;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;

public interface ProcessStream {
    void process(DataStream<String> inputStream) throws IOException;

    }
