from threading import Thread, enumerate
from Queue import Queue
from lxml import etree
import logging
import csv
import psutil
import os
import time
import re

logging.basicConfig(level=logging.DEBUG,
                    format='(%(asctime)-15s) (%(threadName)-10s) %(message)s'
                    )
isfirst = True
threada_pid = 0
threadb_pid = 0
threadc_pid = 0
fieldscoll = ""

class XMLCreator(Thread):

    def __init__(self, numrows, q):
        self.num = numrows
        self.q = q

        Thread.__init__(self)
        self.num = numrows
        self.q = q

    def run(self):

        #create tags and fields for xml builder
        val_pattern = ['0000', 'Name_', '97100000', 'Street Number ',
                       'Country_', '0200000', 'SecondName_',
                       'DriverLicense_ ']
        fields_name = ['ID', 'Name', 'PhoneNumber', 'Address', 'Country',
                       'Fixed Line', 'SecondName', 'DriverLicense']

        #create xml parets objects and tags
        data = etree.Element("Data")
        objectelement = etree.SubElement(data, "Object")

        logging.debug("Streaming " + str(self.num) + " in a csv file")

        #fill xml tree structure
        for i in xrange(0, self.num):

            k = 0
            child = etree.SubElement(objectelement, "Fields", Count=str(i))

            for k in xrange(0, fields_name.__len__()):

                etree.SubElement(child, "Field", Name=fields_name[k]).text = str(val_pattern[k]) + str(i)

            #stream queue element to xml consumer thread
            self.q.put(etree.tostring(child))
            objectelement.remove(child)

        self.q.put(None) #sending notification none when xaml ends


class XMLParser(Thread):

    def __init__(self, numrows, q):
        self.q = q
        self.num = numrows
        self.rows = 0
        self.start
        self.rows1 = 0
        self.fields1 = ""
        Thread.__init__(self)
        self.q = q
        self.num = numrows
        self.rows = 0
        self.start
        self.rows1 = 0
        self.fields1 = ""

    def run(self):

        logging.debug("Ready to receiving xml elements...")

        #deleting the fine if exists
        try:
            os.remove('cpuandmemusage.csv')
        except OSError, e:
            print ("Error: %s - %s." % (e.filename, e.strerror))

        data = self.q.get() #get element in the queue for the first time
        self.start = time.time()

        #receives the stream/queue from xml producer thread
        while data is not None:

            data = self.q.get()
            if data is not None:
                self.writer(data)
            self.q.task_done()
        self.q.task_done()

    def writer(self, xmlelement):

        if self.rows1 < 100:
            self.fields1 = (self.fields1 + re.compile(r'<[^>]+>').sub('', xmlelement) + "\r\n")

        if self.rows1 == 100:
            try:
                with open('cpuandmemusage.csv', "ab+") as f:
                    f.write(self.fields1 + re.compile(r'<[^>]+>').sub('', xmlelement) + "\r\n")
            finally:
                self.rows1 = 0
                self.fields1 = ""
                f.close()

        if self.rows == 1000:
            print(time.time() - self.start)
            self.rows = 0
            self.start = time.time()

        self.rows += 1
        self.rows1 += 1



class ThreadsResourceMonitor(Thread):

    def __init__(self):
        self.interval = 0.1
        self.numlines = 0

        Thread.__init__(self)
        self.interval = 0.1
        self.numlines = 0

    def run(self):
        process = psutil.Process()

        try:
            os.remove('resourcelog.csv')
        except OSError, e:
            print ("Error: %s - %s." % (e.filename, e.strerror))

        lines = []
        num = 0

        while any((t.name is 'Xmlproducer'
                   and t.isAlive()) for t in enumerate()):

            for t in process.threads():

                if t.id == threada_pid:
                    ta_user_time = t.user_time
                    ta_system_time = t.system_time

                elif t.id == threadb_pid:
                    tb_user_time = t.user_time
                    tb_system_time = t.system_time

            total_percent = process.cpu_percent(self.interval)
            total_time = sum(process.cpu_times())

            ta_cpu_percent = total_percent * ((ta_user_time + ta_system_time) / total_time)
            tb_cpu_percent = total_percent * ((tb_user_time + tb_system_time) / total_time)

            py_wse = process.memory_full_info().rss
            py_vms = process.memory_full_info().vms
            pypid = process.pid

            line = [
                "Time: " + str(time.strftime("%a %b %d %H:%M:%S +0000")),
                "PID: " + str(threada_pid),
                "TA_UT: " + str(ta_user_time),
                "TA_ST: " + str(ta_system_time),
                "PID: " + str(threadb_pid),
                "TB_UT: " + str(tb_user_time),
                "TB_ST: " + str(tb_system_time),
                "TA_CPU %: " + str(ta_cpu_percent),
                "TB_CPU %: " + str(tb_cpu_percent),
                "pyPID: " + str(pypid),
                "PS_Working_Set: " + str(py_wse),
                "PS_Virtual_Memory: " + str(py_vms)]

            self.numlines += 1
            lines.append(line)
            num += 1

            if num == 10:
                try:
                    with open("resourcelog.csv", "ab+") as f:
                        csvwriter = csv.writer(f)
                        csvwriter.writerows(lines)
                        num = 0
                        lines = []
                finally:
                    f.close()


def init(numrows):

    global threada_pid
    global threadb_pid
    global threadc_pid

    #initialise queue
    q = Queue(1)

    #create threads
    ThreadA = XMLCreator(numrows, q)
    ThreadB = XMLParser(numrows, q)
    ThreadC = ThreadsResourceMonitor()

    #set threads name
    ThreadA.setName("Xmlproducer")
    ThreadB.setName("XMLConsumer")
    ThreadC.setName("ThreadsResourceMonitor")

    #start threads
    ThreadB.start()
    ThreadA.start()
    ThreadC.start()

    #get the threads pids
    threadb_pid = ThreadB.ident
    threada_pid = ThreadA.ident
    threadc_pid = ThreadC.ident

    #join in a sequence of completion
    q.join()
    ThreadB.join()
    ThreadA.join()
    ThreadC.join()

init(10000)
