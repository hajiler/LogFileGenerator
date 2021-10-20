sbt clean
sbt test
sbt assembly
numPreviousTests=$(ls ./doc/results | wc -l)
hadoop jar target/scala-3.0.2/LogFileGenerator-assembly-0.1.jar input/LogFileGenerator.2021-10-17.log "doc/results/local_test${numPreviousTests}"
