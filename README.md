# How to use this repository?
## Prerequisites - set your environment and IDE

To begin, let's assume you don't have any working environment but only a Linux machine. Follow the steps to prepare your machine, start with installing newer versions of the existing packages on the machine:<br>
```$ sudo apt-get upgrade```

* Check if there is Java on your machine by running the command ```$ java --version```; if not, install the latest package of Java:
```$ sudo apt-get install default-jre```.
* Run the command ```$ java --version``` that should return the latest version.
* Next, install the latest JDK (javac):
``$ sudo apt-get install default-jdk``.<br>
* Verify the Java development kit was installed properly by running the command ``$ javas --version``, it should return the latest version.

Lastly, install an IDE. I chose to use IntelliJ IDEA, but feel free to choose any other IDE [here is a comparison][1].
Follow the instructions in this guide to [install IntelliJ IDEA on Ubuntu][2]

[1]: https://www.javaworld.com/article/3114167/choosing-your-java-ide.html
[2]: https://itsfoss.com/install-intellij-ubuntu-linux

## Setting the project
1. Download the folder flink-demo.
2. Open IntelliJ.
3. Click on Import Project and select the folder you've downloaded from GitHub.
4. Select Maven as the external module.

## Troubleshooting

In case of problems in compiling the project, verify the following:

### Verify the project's definition are correct
1. Verify the correct JVM is associated with the project.
2. Make sure the Java and Flink packages are included in the compilation; otherwise, you might encounter compilation errors.


