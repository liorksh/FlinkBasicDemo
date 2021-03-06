# How to use this repository?
## Prerequisites - set your environment and IDE

To begin, let's assume you don't have any working environment but only a Linux machine. Follow the these steps to set up your machine.
* Start with installing newer versions of the existing packages on the machine:
```$ sudo apt-get upgrade```

* Check if Java is installed on your machine by running the command ```$ java -version```; if not, install the latest package of Java:
```$ sudo apt-get install default-jre```.
* Run the command ```$ java -version``` that should return the latest version.
* Next, install the latest JDK (javac):
``$ sudo apt-get install default-jdk``.
* Verify the Java development kit was installed properly by running the command ``$ javac -version``, it should return the latest version.

Lastly, install an IDE. I chose to use IntelliJ IDEA, but feel free to choose any other IDE [here is a comparison][1].
Follow the instructions in this guide to [install IntelliJ IDEA on Ubuntu][2]

[1]: https://www.javaworld.com/article/3114167/choosing-your-java-ide.html
[2]: https://itsfoss.com/install-intellij-ubuntu-linux

## Setting the project
1. Download this repository or clone it locally.
2. Open IntelliJ.
3. Click on Import Project, select the flink-demo, which is under the root folder you've downloaded from GitHub.
![import project](images/Import-Project.png)
![select project](images/select-project.png)
4. Select Maven as the external module.
![select Maven an external module](images/select-maven.png)

## The project structure and folders

After opening the project witn IntelliJ, select Project Setting --> Modules. On the left side of the window, you should see the following structure and configuration:
1. Project's sources; the hierarchy outlines its structure
![project sources](images/project-sources.png)

2. Project's paths (the targets to the .class files)

![project pathes](images/project-paths.png)

3. Project's dependencies (JVM and associated packages)
![project dependencies](images/project-dependencies.png)

## Compile and Run
After building the project, you can run the main application StreamProcessingApp:

![run the project](images/project-run.png)

The application displays a simple menu for the user to select what kind of stream processing to run, for example: basic processing, split and merge streams, process streams based on a session time-based window.

*If you encounter compilation problems or runtime errors, please refer to the Troubleshooting clause.*

### Project files and functionality

* The main application runs from [StreamProcessingApp class][6].
* Each stream-processing class implemets the [ProcessStream interface][5].
* Examples for the various [stream processes classes][3]: 
  * BasicStreamOperations: process events and save the results into a sink destination (folder).
  * SplitStreamOperation: split stream into more than one stream, while converting the data type. Afterwards, calling a method to merge two streams and produce a new type of stream.
  * TimeBasedWindowOperations: group the stream and process it based on a time-based session window.
* Auxiliary and data object classes are located under the [folder common][4].

[3]: https://github.com/liorksh/FlinkBasicDemo/tree/master/flink-demo/src/main/java/flinkdemo/process
[4]: https://github.com/liorksh/FlinkBasicDemo/tree/master/flink-demo/src/main/java/flinkdemo/common
[5]: https://github.com/liorksh/FlinkBasicDemo/blob/master/flink-demo/src/main/java/flinkdemo/process/ProcessStream.java
[6]: https://github.com/liorksh/FlinkBasicDemo/blob/master/flink-demo/src/main/java/flinkdemo/process/StreamProcessingApp.java

## Troubleshooting 

In case of problems in compiling or running the project, verify the following:

### Verify the project's definitions are correct
1. Verify the correct JVM is associated with the project.

![JVM](images/project-JVM.png)

2. Make sure the Java and Flink packages are included in the compilation, and change the packages scope to "Compile"; otherwise, you might encounter compilation errors, for example:

![error compile packages](images/error-compile-packages.png)

To rectify the problem, chaneg the packages scope to compile via the GUI (you can also do it by amending the file FlinkStreamingDemo.iml):

![compile packages](images/compile-packages.png)

3. If the pom.xml file cannot be found, you will receive the alert below; then, set the project as a Maven project.

![set as a Maven project](images/set-maven-project.png)

4. Another alert might be related to importing Maven dependencies. Setting Auto-Import can assist.

![import Maven](images/maven-import.png)

## Play with it and give feedback

Hope you find this repository useful; feel free to engage.




