import xmltodict
import pprint
import json
import clsSQLServer
from datetime import datetime
import tzlocal
from tkinter import Tk
from tkinter.filedialog import askopenfilename


def user_get_file():
    Tk().withdraw()
    source_file = askopenfilename(initialdir='C://Users//Andy//Downloads')
    return source_file


def write_to_db(chonk:list):
    s = clsSQLServer.Interface(database='Convo')
    sql = \
"""insert into Convo.dbo.load_conv(source_timestamp, converted_timestamp, author, body) 
values (?, ?, ?, ?);"""
    s.InsertMany(sql, chonk)


def load_db():
    s = clsSQLServer.Interface(database='Convo')
    sql = 'exec dbo.load_conversation_data'
    s.Execute(sql)


def main():

    def do_append(unix_timestamp, author, text):
        local_timezone = tzlocal.get_localzone()
        local_time = datetime.fromtimestamp(float(unix_timestamp)/1000, local_timezone)
        all_data.append([unix_timestamp, local_time, author, text])

    source_file = user_get_file()
    all_data = []

    print('reading file data...')
    with open(source_file, 'r', encoding='utf-8') as f:
        my_xml = f.read()

    print(f'read {len(my_xml)} thingies')
    s0 = xmltodict.parse(my_xml)
    s1 = s0['smses']

    print('parsing content...')
    for k,v in s1.items():
        if k == 'mms':
            counter = 0
            for vv in v:
                date = vv["@date"]
                for part in vv["parts"].values():
                    author = 'Rebecca'
                    if isinstance(part, list):
                        for od in part:
                            seq_flag = int(od['@seq'])
                            if seq_flag == -1:
                                author = 'Andy'
                            else:
                                text = od["@text"]
                    elif isinstance(part, dict):
                        if part["@seq"] == '-1':
                            author = 'Andy'
                        text = part["@text"]
                    else:
                        author = '<unknown>'
                        text = '<unknown>'
                # all_data.append([date, datetime.utcfromtimestamp(float(date)/1000).isoformat(), author, text])
                do_append(date, author, text)
        elif k == 'sms':
            for vv in v:
                author = 'Rebecca' # I don't actually know how to determine the author for sms message types
                date = vv["@date"]
                text = vv["@body"]
            # all_data.append([date, datetime.utcfromtimestamp(float(date)/1000).isoformat(), author, text])
            do_append(date, author, text)
        elif type(v) == 'str':
            pass
        else:
            pass
    print(f'{len(all_data)} records read\n')
    print('writing to database...')
    write_to_db(all_data)
    load_db()
    print('done')

if __name__ == '__main__':
    main()
