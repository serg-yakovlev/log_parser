import zipfile
import os
import re
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor



class Parser():
    
    def __init__(self):
        cpu = os.cpu_count()
        file_list = os.listdir(".")
        
        def extract(file):
            if zipfile.is_zipfile(file):
                z = zipfile.ZipFile(file, 'r')
                z.extractall()
                return '{0} file {1}'.format(datetime.now()-start, file)
            
        with ThreadPoolExecutor(max_workers=cpu * 2) as executor:
            iter_obj = executor.map(extract, file_list)
        logs = [elem for elem in iter_obj if elem]
        logs.sort()
        for l in logs:
            print(l)            
        file_list = os.listdir(".")
        log_list = []
        for file in file_list:
            if re.match(r'metrics-\d\d\d\d-\d\d-\d\d\.\d+\.log$', file):
                log_list.append(file)
        self.log_list = log_list
        self.result = []
        self.sort()    

    def sort(self):
        
        def add_rows(file):
            l = Logfile(file)
            new_records = l.split()
            if not new_records:
                return '{0} file {1} {2} symb, no valid records'.format(datetime.now()-start, file, len(l.text))
            else:
                records = new_records
            for elem in records:
                rec = Record(elem["date"], elem["record"])
                if rec.record:
                    self.result.append(rec)
            return '{0} file {1} {2} symb {3} records, {4} records added'.format(datetime.now()-start, file, len(l.text), len(l.records), len(records))
                
        cpu = os.cpu_count()
        with ThreadPoolExecutor(max_workers=cpu * 2) as executor:
            iter_obj = executor.map(add_rows, self.log_list)
        logs = [elem for elem in iter_obj if elem]
        logs.sort()
        for l in logs:
            print(l) 
        self.result.sort(key=lambda x: [x.record['date'], x.record['timestamp']])



    def create_json(self):
        current = None
        result = []
        for elem in self.result:
            current = elem if elem.state == 'RUN_POSITION' else current
            if elem.state == 'BUILD':
                result.append({"run": current.parse(), "build": elem.parse()})
                current = None
        with open('logs_reg.json', "w", encoding="utf-8") as file:
            json.dump(result, file)
        return result

            
class Logfile():

    def __init__(self, file):
        self.file = file
        with open(file) as f:
            self.text = f.read()
            cpu = os.cpu_count()     
            
    def is_valid(self):
        return ('RUN_POSITION' in self.text and 'BUILD' in self.text)

    def split(self):
        if self.is_valid():
            patternD = (r'(\d{4}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3})'
                        '(?:\s(?!\d{4}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3})|'
                        '\S(?!\d{4}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3}))*(?:RUN_POSITION|BUILD)'
                        '(?:\s(?!\d{3}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3})|'
                        '\S(?!\d{3}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3}))*')
            patternR = (r'\d{4}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3}'
                        '((?:\s(?!\d{4}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3})|'
                        '\S(?!\d{4}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3}))*(?:RUN_POSITION|BUILD)'
                        '(?:\s(?!\d{3}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3})|'
                        '\S(?!\d{3}-\d{2}-\d{2}[\s]+\d{2}:\d{2}:\d{2}\.\d{3}))*)')
            self.records = re.findall(patternR, self.text)
            self.dates = re.findall(patternD, self.text)
            records = []
            for d, r in zip(self.dates, self.records):
                #if "RUN_POSITION" in r or "BUILD" in r: 
                records.append({"record": r, "date": d})
            return records
        else:
            self.records = []
            return None

    
class Record():

    def __init__(self, date, record):
        if not ('RUN_POSITION' in record or 'BUILD' in record):
            self.record = None
            return        
        try:
            j = re.search(r'{[\s\S]*}', record)
            js = json.loads(j.group(0))
            self.state = js['payload']['state']
            ts = js['timestamp']            
            self.record = {"date": date, "text": record, "timestamp": ts, "json": js}
        except Exception:
            self.record = None

    def parse(self):
        if not self.record:
            return None
        else:
            if self.state == 'RUN_POSITION':
                result = {
                            "date": self.record['date'],
                            "state": self.state,
                            "trajectoryPositions": (self.record['json']['payload']['trajectoryPositions'] if 'trajectoryPositions' in self.record['text'] else None),
                            "trajectoryPoses": (self.record['json']['payload']['trajectoryPoses'] if 'trajectoryPoses' in self.record['text'] else None)
                            }
            else:
                result = {
                            "date": self.record['date'],
                            "state": self.state,
                            "builtPoses": (self.record['json']['payload']['builtPoses'] if 'builtPoses' in self.record['text'] else None),
                            "timestamp": self.record['timestamp']
                            }
            return result


start = datetime.now()
p = Parser()
js = p.create_json()
#print(js[10]['run'])
#print(js[10]['build'])
#print(js[1000]['run'])
#print(js[1000]['build'])

finish = datetime.now()

print(finish - start)
