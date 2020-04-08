package flinkdemo.common;

import java.io.Serializable;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/*
This class implements a timestamp extractor; the watermark is set based on a fixed maximum time frame.
 */
public class TimestampExtractor implements AssignerWithPeriodicWatermarks<String>{

    private final long maxTimeFrame = 1500;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis()-maxTimeFrame);
    }

    @Override
    public long extractTimestamp(String str, long l) {
        return InputData.getDataObject(str).timestamp;
    }
}
