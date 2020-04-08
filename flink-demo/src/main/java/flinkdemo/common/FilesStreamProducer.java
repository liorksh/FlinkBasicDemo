package flinkdemo.common;

import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.util.*;


/*
This class generates file into a given folder.
The Run method allows the process to repeat or exist, based on the user's input.
 */
public class FilesStreamProducer implements Runnable {

    // The default number of files to generate
    private  static Integer numOfInputs = 10;

    // Holds the directory to output the files
    private String folderPath;

    public FilesStreamProducer(Integer _cycles, String _folderPath){
        folderPath = _folderPath;
        numOfInputs = _cycles;
    }

    public void run() {

        try {

            // Ensuring the destination folder exists
            Utils.ensureFolderExists(folderPath);

            // Define a list of names
            List<String> names = new ArrayList<String>(
                Arrays.asList("Paul", "John", "George", "Ringo"));

            //   Define a list of operations
            List<String> appOperation = new ArrayList<String>(
                    Arrays.asList("Bass", "Lead", "Rhythm", "Drums","Keyboards"));

            //Define list of application entities
            List<String> appEntity = new ArrayList<String>();
            appEntity.add("Singer");
            appEntity.add("Player");
            appEntity.add("Producer");

            Utils.ensureFolderExists(folderPath);

            // Use a random object
            Random random = new Random();

            // Generate sample data records, two per each file
            for(int i=0; i < numOfInputs; i++) {

                // Create a text array, with the content
                List<String> csvFormatText = new ArrayList<String>(
                   Arrays.asList(String.valueOf(i),
                           names.get(random.nextInt(names.size())),
                           appEntity.get(random.nextInt(appEntity.size())),
                           appOperation.get(random.nextInt(appOperation.size())),
                           String.valueOf(System.currentTimeMillis()),
                           String.valueOf(random.nextInt(10) + 1 ),
                           System.lineSeparator(), // a new line

                           String.valueOf(++i),
                           names.get(random.nextInt(names.size())),
                           appEntity.get(random.nextInt(appEntity.size())),
                           appOperation.get(random.nextInt(appOperation.size())),
                           String.valueOf(System.currentTimeMillis()+i), // create a unique time stamp since the two items are created together
                           String.valueOf(random.nextInt(10) + 1 ))) ;


                // Open a new file for writing the content
                FileWriter eventFile = new FileWriter(folderPath
                                            + "/item_" + i + ".csv");
                CSVWriter eventCSV = new CSVWriter(eventFile);

                // Write the data record and close the file
                eventCSV.writeNext(csvFormatText.toArray(new String[0]));

                Utils.print(Utils.COLOR_BLUE, "Creating item: "
                            + Arrays.toString(csvFormatText.toArray()));

                eventCSV.flush();
                eventCSV.close();

                // Sleep for a random time the next cycle.
                Thread.sleep(random.nextInt(3000) + 1);
            }

            // Once one cycle is finished, the user can select to start another one or quit.
            System.out.println("Finished one cycle of input generator. Waiting for instructions:\n - 'c' to start another cycle\n - 'e' to exit");

            Scanner scanner = new Scanner(System.in);
            String str = scanner.next();

            // Decide whether to run another cycle based on the user's choice.
            if(str.startsWith("c")) {
                System.out.println("Starting another cycle");
                run();
            }if(str.startsWith("e")) {
                System.exit(0);
            }else {
                System.out.println("Unknown input "+str);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
