sbt clean
sbt assembly
numPreviousTests=$(ls ./output | wc -l)
hadoop jar target/scala-3.0.2/LogFileGenerator-assembly-0.1.jar input/LogFileGenerator.2021-10-05.log "output/test${numPreviousTests}"
