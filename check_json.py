import re
import json
import os


with open('logs.json') as f:
    js = json.loads(f.read())

def check_records():
    bsum = 0
    rsum = 0
    file_list = os.listdir(".")
    for file in file_list:
        if re.match(r'metrics-\d\d\d\d-\d\d-\d\d\.\d+\.log$', file):          
            with open(file) as f:
                text = f.read()
                b = len(re.findall(r'BUILD', text))
                r = len(re.findall(r'POSITION', text))
                bsum += b
                rsum += r
                print(file, 'BUILD: ', b, 'RUN: ', r, 'ALL: ', b+r)
    return [bsum, rsum]

    
def print_dates():
    dates=[{'run':elem['run']['date'], 'build':elem['build']['date']} for elem in js][0:]
    for n, d in enumerate(dates, 1):
        print(n, d)


date = '2019-06-12'


def find_date(date):
    res = [elem for elem in js if date in elem['run']['date'] or date in elem['build']['date']]
    print(len(res))
    print(res[0]['build']['timestamp'])
    print(res[0]['run'], '\n\n')
    print(res[0]['build'])


#print_dates()
#find_date('date')
print(check_records())





