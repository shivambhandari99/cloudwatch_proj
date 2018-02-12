# Cloudwatch_proj_intern
Cloudwatch to prometheus to grafana.

The script cwMetricFetch fetches metrics described in the data_consolidated pickle and uses the python client for prometheus to publish this extracted data on an http endpoint(9106). Prometheus has to be configured in order to scrape data from this endpoint.

## Brief description of the classes and functions

#### get_stat_thread (class)
This class inherits from threading.thread. The logic of the fetch is in the overridden run function. Python threads work pretty well for concurrent I/O so for each getMetricStatistic API call a new thread is created. At a particular point of time not more than 500 threads are formed in order to avoid throtling from cloudwatch.

#### main
In the main part of the program we initiate the threads, create gauges(cloudwatch datatypes which can have variable values) and store the gauge handlers in a dictionary with metric related properties as the key(specifically the string repr of the list containing namespace, metric name and dimension names). A timeout is set for join as some getMetricStatistics API calls fail. These failed threads are not killed as py threads cannot be killed asynchronously. They are kept as dangling and when the timeout for the API call is met it automatically exits. Meanwhile the other threads complete their execution, and we collect info about all the failed threads(metric info). These metrics are again retried in a threading fashion(as we can restrict the time it takes by having a timeout) one at a time, again and again till all metric values are obtained. For 370 metrics it takes approximately 100 seconds.

The last line has the sleep function which controlls the frequency of the API calls(currently set to 30 mins).
