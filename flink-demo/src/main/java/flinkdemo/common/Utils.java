package flinkdemo.common;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public  class Utils {

    public static final String COLOR_RESET = "\u001B[0m";
    public static final String COLOR_GREEN = "\u001B[32m";
    public static final String COLOR_CYAN = "\u001B[36m";
    public static final String COLOR_BLUE = "\u001B[34m";
    public static final String COLOR_YELLOW = "\u001B[33m";

    public static void printMessage(String msg) {
        print(COLOR_YELLOW, msg);
        System.out.println("-----------------------------------");
    }

    public static void print(String color, String msg) {
        System.out.println(String.format("%s %s %s", color, msg, COLOR_RESET));
    }

    // Ensure the folder exists; otherwise, create the folder.
    public  static void ensureFolderExists(String folderPath) throws IOException {
        if(Files.exists(Paths.get(folderPath))==false){
            Files.createDirectory(Paths.get(folderPath));
        }
        else{
            // Clean out the directory
            FileUtils.cleanDirectory(new File(folderPath));
        }
    }
}
