### MAPPER
I designed my Map/Reduce job to use Text as a key, and IntWritable as the value. In each Mapper, a log is parsed an 
analyzed. It is mapped to the 10-second time interval at which occurred (i.e a log that occurred at 18:43:43 is mapped 
to the time interval 18:43:40-18:43:49). Then, if the log message contains a regex injection, this time interval for that 
log type is mapped to the value 1 for an injected string. And the log type is mapped to the number of characters in the 
message.Otherwise --if the log message does contain a regex injection-- it is mapped to the value 1 for that time 
interval for that log type, not containing injections. If the log type is of Error and there is an injected string then
an additional key for that time interval is mapped to the value 1. 

### REDUCER
Then during reducer tasks there are two situations. If the key being reduced is the max character count, in which case
the max character count of that log type is reduced to the maximum int from the values that are input for that reducer.
For all other situations, we are merely keeping track of the total number of mappings, so the key is reduced to the sum
of occurrences of that key (i.e if there are 12 mappings of the ERROR log for non injected strings during the interval 
18:43:40-18:43:49, than reduced value for this key is 12).

### OUTPUT
The output of the entire job contains 4 sections. The first is each interval (every 10 seconds from the start of the
log file to the end of the log file) and the number of occurrences of each log type for that interval for injections 
and no injections. The second section is the descending time intervals that contain error logs with the injected string,
and number of occurrences of these logs. The 3rd section is max character count of log messages containing the injected
string for each log type. And the final section is the total number of messages for each log type in the input file.