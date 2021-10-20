Sbt Version : 1.1 Scala Version : 3.0.2 Java Version : 11.0.11

Clone github repository on local machine using IntelliJ import from version control feature.  

To run unit tests, build jar file, and run local map reduce job execute:

    ./runjob.sh

If there is an issue running the script execute the following commands:

    sbt clean
    sbt test
    sbt assembly
    numPreviousTests=$(ls ./doc/results | wc -l)
    hadoop jar target/scala-3.0.2/LogFileGenerator-assembly-0.1.jar input/LogFileGenerator.2021-10-17.log "doc/results/local_test${numPreviousTests}"__