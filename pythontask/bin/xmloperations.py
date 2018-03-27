from threading import Thread, enumerate
from Queue import Queue
from lxml import etree
import logging
import csv
import psutil
import os
import time

logging.basicConfig(level=logging.DEBUG,
                    format='(%(asctime)-15s) (%(threadName)-10s) %(message)s'
                    )
isfirst = True
threada_pid = 0
threadb_pid = 0
threadc_pid = 0

class XMLCreator(Thread):

    def __init__(self, numrows, q):
        self.num = numrows
        self.q = q

        Thread.__init__(self)
        self.num = numrows
        self.q = q

    def run(self):

        #create tags and fields for xml builder
        val_pattern = ['0000', 'Name_', '97100000', 'Street Number ', 'Country_', '0200000', 'SecondName_',
                       'DriverLicense_ ']
        fields_name = ['ID', 'Name', 'PhoneNumber', 'Address', 'Country', 'Fixed Line', 'SecondName', 'DriverLicense']

        #create xml parets objects and tags
        data = etree.Element("Data")
        objectelement = etree.SubElement(data, "Object")

        logging.debug("Streaming " + str(self.num) + " in a csv file")

        #fill xml tree structure
        for i in xrange(0, self.num):
            k = 0
            child = etree.SubElement(objectelement, "Fields", Count=str(i))
            for k in range(0, fields_name.__len__()):
                etree.SubElement(child, "Field", Name=fields_name[k]).text = str(val_pattern[k]) + str(i)

            #stream queue element to xml consumer thread
            self.q.put(etree.tostring(child, xml_declaration=True, pretty_print=True))
            objectelement.remove(child)
        self.q.put(None) #sending notification none when xaml ends


class XMLParser(Thread):

    def __init__(self, q):
        self.q = q

        Thread.__init__(self)
        self.q = q

    def run(self):

        logging.debug("Ready to receiving xml elements...")

        #deleting the fine if exists
        if os.path.exists("testxmlmethod.csv"):
            os.remove("testxmlmethod.csv")
        else:
            print("Sorry, I can not remove %s file." % "testxmlmethod.csv")

        data = self.q.get() #get element in the queue for the first time

        #receives the stream/queue from xml producer thread
        while data is not None:
            data = self.q.get()
            if data is not None:
                self.writer(data)
            self.q.task_done()
        self.q.task_done()

    def writer(self, xmlelement):
        global isfirst

        xml = etree.fromstring(xmlelement)
        fieldelements = etree.ElementTree(xml)
        root = fieldelements.getroot()
        head = []
        fields = []
        f = open('testxmlmethod.csv', "ab+")
        csvwriter = csv.writer(f, lineterminator="\r")

        for element in fieldelements.iter("Field"):
            fields.append(element.text)
            if isfirst is True:
                head.append(element.get("Name"))
                if len(head) == len(root.xpath("//*"))-1:
                    csvwriter.writerow(head)
                    isfirst = False
        csvwriter.writerow(fields)
        f.close()

class ThreadsResourceMonitor(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.interval = 0.1
        self.numlines = 0
        self.deletefile()
        self.f = open("cpuandmemusage.csv", "ab+")


    def run(self):
        process = psutil.Process()

        while any((t.name is 'Xmlproducer' and t.isAlive()) for t in enumerate()):

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

            dict = [
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
            print(str(self.numlines) + str(dict))
            self.csvwriter().writerow(dict)
            self.f.flush()
        self.f.close()

    def csvwriter(self):
        return csv.writer(self.f, delimiter="|", lineterminator="\r")

    def deletefile(self):
        if os.path.exists('cpuandmemusage.csv'):
            os.remove('cpuandmemusage.csv')
        else:
            print("Sorry, I can not remove %s file." % 'cpuandmemusage.csv')


def init(numrows):

    global threada_pid
    global threadb_pid
    global threadc_pid

    #initialise queue
    q = Queue(1)

    #create threads
    ThreadA = XMLCreator(numrows, q)
    ThreadB = XMLParser(q)
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


init(1000000000)


