import boto3
import timeit
from datetime import datetime, timedelta
import threading
import prometheus_client as pc
import sys
import time
import math
import re
import pickle

global_lock = threading.Lock()

class monitor(threading.Thread) :
    def run(self) :
        global signal
        while(1) :
            self.tt=threading.Timer(1.0,self.time)
            self.tt.start()
            self.tt.join()
            if signal == 1 :
                break

    def time(self) :
        global count
        global global_lock
        global_lock.acquire()
        count=0
        global_lock.release()

class get_stat_thread(threading.Thread) :

    local_lock = threading.Lock()

    def __init__(self,number,metric_page,key) :
        threading.Thread.__init__(self)
        self.metric_number=number
        self.metric_page = metric_page
        self.key = key
        #self.metric_page=metric_page

    def copy(self,res) :
        self.metric_page = res.metric_page

    def run(self) :
        global count
        global global_lock
        dimension_list=[]
        dimension_dict={}
        #dd=[]
        for i in self.metric_page['Metrics'][self.metric_number]['Dimensions'] :
            dimension_list.append(i)
            dimension_dict[i['Name']]=i['Value']
            #dd.append(i['Value'])
        curr_time = datetime.utcnow()
        minu = curr_time.minute
        if(minu%10>0 and minu%10<5) :
            minu=minu+(5-minu%10)
        elif (minu%10>5 and minu%10 < 10) :
            minu=minu+(10-minu%10)
        if minu==60 :
            minu=55
        curr_time=curr_time.replace(minute=minu)
        try :
            self.response = cloudwatch.get_metric_statistics(
                Namespace=self.metric_page['Metrics'][self.metric_number]['Namespace'],
                Dimensions=dimension_list,
                MetricName=self.metric_page['Metrics'][self.metric_number]['MetricName'],
                StartTime=curr_time - timedelta(minutes=30),
                EndTime=datetime.utcnow(),
                Period=1800,
                Statistics=[
                    'Average','Maximum','Minimum','Sum'
                ],
            )
        except :
            return
        try :
            self.response_p90 = cloudwatch.get_metric_statistics(
                Namespace=self.metric_page['Metrics'][self.metric_number]['Namespace'],
                Dimensions=dimension_list,
                MetricName=self.metric_page['Metrics'][self.metric_number]['MetricName'],
                StartTime=curr_time - timedelta(minutes=30),
                EndTime=datetime.utcnow(),
                Period=1800,
                ExtendedStatistics=[
                    'p90'
                ],
            )
        except :
            return
        if len(self.response['Datapoints'])==1 :
            #Some metrics are without dimensions.
            dimension_dict['stat'] = 'Average'
            existing_gauges[self.key].labels(**dimension_dict).set(self.response['Datapoints'][0]['Average'])
            dimension_dict['stat'] = 'Maximum'
            existing_gauges[self.key].labels(**dimension_dict).set(self.response['Datapoints'][0]['Maximum'])
            dimension_dict['stat'] = 'Minimum'
            existing_gauges[self.key].labels(**dimension_dict).set(self.response['Datapoints'][0]['Minimum'])
            dimension_dict['stat'] = 'Sum'
            existing_gauges[self.key].labels(**dimension_dict).set(self.response['Datapoints'][0]['Sum'])
        elif len(self.response['Datapoints'])==0 :
            dimension_dict['stat'] = 'Average'

            existing_gauges[self.key].labels(**dimension_dict).set(float('nan'))
            """except :
                print(dimension_dict)
                print(existing_gauges[self.key]._labelnames)
                print(self.key)"""
            dimension_dict['stat'] = 'Maximum'
            existing_gauges[self.key].labels(**dimension_dict).set(float('nan'))
            dimension_dict['stat'] = 'Minimum'
            existing_gauges[self.key].labels(**dimension_dict).set(float('nan'))
            dimension_dict['stat'] = 'Sum'
            existing_gauges[self.key].labels(**dimension_dict).set(float('nan'))
        if len(self.response_p90['Datapoints'])==1 :
            dimension_dict['stat'] = 'p90'
            existing_gauges[self.key].labels(**dimension_dict).set(self.response_p90['Datapoints'][0]['ExtendedStatistics']['p90'])
        elif len(self.response_p90['Datapoints'])==0 :
            dimension_dict['stat'] = 'p90'
            existing_gauges[self.key].labels(**dimension_dict).set(float('nan'))

    #This function can be used to log or just print the  data on std output.
    def print_data(self) :
        print("Metric Name - ", self.metric_page['Metrics'][self.metric_number]['MetricName'])
        print("Dimensions - ")
        for j in self.metric_page['Metrics'][self.metric_number]['Dimensions'] :
            print(j['Name'],"-",j['Value'])
        for j in self.response['Datapoints'] :
            print("    Timestamp : ",j['Timestamp'])
            print("    Average : ",j['Average'])

    #stores the datapoints for future use (without timestamp)
    def store_datapoints(self) :
        dim_list=[]
        datapoints_dict={}
        for j in self.response['Datapoints'] :
            dim_list.append(j)
        datapoints_dict['name'] = self.metric_page['Metrics'][self.metric_number]['MetricName']
        datapoints_dict['dimensions'] = self.metric_page['Metrics'][self.metric_number]['Dimensions']
        datapoints_dict['datapoints'] = dim_list
        return datapoints_dict


cloudwatch = boto3.client('cloudwatch')

paginator = cloudwatch.get_paginator('list_metrics')

pages = []

pc.start_http_server(9106)


#logic to collect metrics
"""start = timeit.default_timer()
print("Started collecting metric details : ")
count_for_paginator=0
for page in paginator.paginate(Namespace='AWS/SQS') :
    count_for_paginator = count_for_paginator+1
    pages.append(page)
    if count_for_paginator==4 :
        break
print("Finished collecting metric details : ")
stop = timeit.default_timer()
print(stop-start)"""

file_Name = "data_consolidated"
# open the file for writing
fileObject = open(file_Name,'rb')
start = timeit.default_timer()
print("Started collecting metric details : ")
"""with open("data.yaml", 'r') as stream:
    try:
        pages = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)"""
pages = pickle.load(fileObject)
stop = timeit.default_timer()
print("Finished collecting metric details : ")
print(stop-start)



print("Starting collecting data : ")

threads=[]
data= []
count=0
signal=0
missed_metrics=[]
missed_metrics_retrieval=[]
metric_gauge_prob=[]
existing_gauges = {}
experimental = {}
critical=0
red=0
repetitive={}
api_calls = 0

for i in range(8) :
    repetitive[i]=0

while(True) :
    retry_counter=0
    start_t_per_iter = timeit.default_timer()
    for page_number in range(int(len(pages))) :
        threads=[]
        for metric_number in range(len(pages[page_number]['Metrics'])) :
            per_thread_info=[]
            temp_list=[]
            metric_name = pages[page_number]['Metrics'][metric_number]['MetricName'].replace('-','_').replace('/','_').replace('.','_')
            key_dict = [pages[page_number]['Metrics'][metric_number]['Namespace'],metric_name+'_Average']
            for j in pages[page_number]['Metrics'][metric_number]['Dimensions'] :
                key_dict.append(j['Name'])
                temp_list.append(j['Name'])
            temp_list.append('stat')
            if repr(key_dict) not in existing_gauges :
                try:
                    gauge = pc.Gauge(metric_name,'',temp_list)
                    existing_gauges[repr(key_dict)]=gauge
                except:
                    metric_gauge_prob.append(pages[page_number]['Metrics'][metric_number])
                    continue
            thread = get_stat_thread(metric_number,pages[page_number],repr(key_dict))
            per_thread_info.append(thread)
            per_thread_info.append(metric_number)
            per_thread_info.append(pages[page_number])
            per_thread_info.append(repr(key_dict))
            threads.append(per_thread_info)
            api_calls=api_calls+2
            thread.start()

        for thread_info in threads :
            thread_handler = thread_info[0]
            metric_number = thread_info[1]
            page = thread_info[2]
            key = thread_info[3]
            thread_handler.join(1)
            #to determine the pattern in missed metric - if repetitive
            if thread_handler.isAlive() :
                missed_metrics.append(page['Metrics'][metric_number])
                missed_metrics_retrieval.append(thread_info)
                if (repr(page['Metrics'][metric_number])) not in experimental :
                    experimental[repr(page['Metrics'][metric_number])]={}
                    experimental[repr(page['Metrics'][metric_number])]["issue"] = 1
                    experimental[repr(page['Metrics'][metric_number])]["solved"] = 0
                else :
                    experimental[repr(page['Metrics'][metric_number])]["issue"] = experimental[repr(page['Metrics'][metric_number])]["issue"] + 1
                value=experimental[repr(page['Metrics'][metric_number])]["issue"]
                if(value not in repetitive) :
                    repetitive[value]=1
                else :
                    repetitive[value]=repetitive[value]+1
                continue
        #Logic for retrival of missed metrics.
    while(len(missed_metrics_retrieval)>0) :
        retry_counter=retry_counter+1
        print("Retry attempt number - ",retry_counter)
        print("Number of metrics to retry - ",len(missed_metrics_retrieval))
        threads=[]
        for missed_metric_info in missed_metrics_retrieval :
            per_thread_info=[]
            metric_number = missed_metric_info[1]
            page = missed_metric_info[2]
            key = missed_metric_info[3]
            thread = get_stat_thread(metric_number,page,key)
            per_thread_info.append(thread)
            per_thread_info.append(metric_number)
            per_thread_info.append(page)
            per_thread_info.append(key)
            threads.append(per_thread_info)
            api_calls=api_calls+2
            thread.start()
        missed_metrics=[]
        missed_metrics_retrieval = []
        for thread_info in threads :
            thread_handler = thread_info[0]
            metric_number = thread_info[1]
            page = thread_info[2]
            key = thread_info[3]
            thread_handler.join(4)
            if thread_handler.isAlive() :
                missed_metrics.append(page['Metrics'][metric_number])
                missed_metrics_retrieval.append(thread_info)
                continue
            else :
                experimental[repr(page['Metrics'][metric_number])]["solved"] = experimental[repr(page['Metrics'][metric_number])]["solved"] + 1
            #else :
                #thread_datapoints = thread_handler.store_datapoints()
                #data.append(thread_datapoints)


    stop = timeit.default_timer()
    print("Total time  : ",stop-start_t_per_iter)
    print("Total api calls : ",api_calls)
    print("Total missed metrics : ",len(missed_metrics))
    print("Total gauge prob metrics : ",len(metric_gauge_prob))
    print("Total missed metrics(2) : ",repetitive[1])
    print("Total missed metrics(2) : ",repetitive[2])
    print("Total missed metrics(3) : ",repetitive[3])
    print("Total missed metrics(4) : ",repetitive[4])
    print("Total missed metrics(5) : ",repetitive[5])
    print("Total missed metrics(6) : ",repetitive[6])
    print("Total missed metrics(7) : ",repetitive[7])
    """ky = experimental.keys()
    for metric_miss_key in ky :
        print(metric_miss_key,":")
        print("No. of misses :",experimental[metric_miss_key]["issue"])
        print("No. of times solved",experimental[metric_miss_key]["solved"],"\n")"""

    time.sleep(1800-(stop-start_t_per_iter))



print("Total time  : ",stop-start)
print("total metrics : ",len(data))
print(len(missed_metrics))
for missed_metric in missed_metrics:
    print(missed_metric)
