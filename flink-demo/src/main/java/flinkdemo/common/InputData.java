package flinkdemo.common;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;

/*
This class encapsulate the object of each event in the stream.
It has a method to convert a set of fields into an InputData object.
* */
public class InputData {

    int id;
    String name;
    String type;
    String feature;
    long timestamp;
    int score;
    //int count;

    // Convert String array to InputData object
    public InputData(String[] attributes) {

        //Assign values
        this.id = Integer.valueOf(attributes[0]);
        this.name = attributes[1];
        this.type = attributes[2];
        this.feature = attributes[3];
        this.timestamp = Long.valueOf(attributes[4]);
        this.score = Integer.valueOf(attributes[5]);
    }


    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getFeature() {
        return feature;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getScore() {
        return score;
    }

    @Override
    public String toString() {
        return "Data {" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", feature='" + feature + '\'' +
                ", timestamp=" + timestamp +
                ", score=" + score +
                '}';
    }

    // Converting a string into an InoutData object
    public static InputData getDataObject(String inputStr) {
        // Split the string
        String[] attributes = inputStr
                .replace("\"","")
                .split(",");

        // Ignore empty strings
        if(StringUtil.isNullOrEmpty(attributes[0]))
          attributes = ArrayUtils.remove(attributes,0);

        return new InputData(attributes);
    }
}
