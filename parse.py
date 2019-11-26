import zipfile
import os
import re
from datetime import datetime
import json


class Parser():
    def __init__(self):
        file_list = os.listdir(".")
        for file in file_list:
            if zipfile.is_zipfile(file):
                z = zipfile.ZipFile(file, 'r')
                z.extractall()
        file_list = os.listdir(".")
        log_list = []
        for file in file_list:
            if re.match(r'metrics-\d\d\d\d-\d\d-\d\d\.\d+\.log$', file):
                log_list.append(file)
        self.log_list = log_list
        self.sort()

    def sort(self):
        records = []
        for file in self.log_list:
            l = Logfile(file)
            new_records = l.split()
            if new_records:
                records += new_records
            else:
                print(datetime.now()-start, file, "No rows")
        result = []
        for elem in records:
            rec = Record(elem["date"], elem["record"])
            if rec.record:
                result.append(rec)
        result.sort(key=lambda x: [x.record['date'], x.record['timestamp']])
        self.result = result
        return self.result

    def create_json(self):
        current = None
        result = []
        for elem in self.result:
            current = elem if elem.state == 'RUN_POSITION' else current
            if elem.state == 'BUILD':
                result.append({"run": current.parse(), "build": elem.parse()})
                current = None
        #print(len(result))
        with open('logs.json', "w", encoding="utf-8") as file:
            json.dump(result, file)
        return result

            
class Logfile():

    def __init__(self, file):
        self.file = file
        with open(file) as f:
            self.text = f.read()
            cpu = os.cpu_count()
            print(len(self.text))              
            
    def is_valid(self):
        return ('RUN_POSITION' in self.text and 'BUILD' in self.text)

    def split(self):
        if self.is_valid():
            print(datetime.now()-start)
            pattern = r'\d\d\d\d-\d\d-\d\d[\s]+\d\d:\d\d:\d\d\.\d\d\d'
            self.records = re.split(pattern, self.text)[1:]
            self.dates = re.findall(pattern, self.text)
            print(datetime.now()-start, self.file, len(self.records), 'rows splited')            
            records = []
            for d, r in zip(self.dates, self.records):
                if "RUN_POSITION" in r or "BUILD" in r: 
                    records.append({"record": r, "date": d})
            print(datetime.now()-start, self.file, len(records), 'rows added')
            return records
        else:
            #print('No valid records in this file')
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
