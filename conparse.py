import xmltodict
import pprint
import json
import clsSQLServer
from datetime import datetime
import tzlocal
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from os import walk
from os import remove
from sys import exit


local_timezone = tzlocal.get_localzone()


def get_file_names_from_repository():
    backup_location = "H:/OneDrive/Apps/SMS Backup and Restore"
    for subdir, _, files in walk(backup_location):
        for filename in files:
            yield f'{subdir}/{filename}'


def user_get_file():
    Tk().withdraw()
    source_file = askopenfilename(initialdir='C://Users//Andy//Downloads')
    return source_file


def write_to_db(chonk:list):
    s = clsSQLServer.Interface(database='Convo')
    d = datetime.now(local_timezone)
    for i in chonk:
        i.append(d)
    # print(chonk)
    sql = \
"""insert into Convo.dbo.load_conv(source_timestamp, converted_timestamp, author, body, record_created) 
values (?, ?, ?, ?, ?);"""
    s.InsertMany(sql, chonk)


def load_db():
    s = clsSQLServer.Interface(database='Convo')
    sql = 'exec dbo.load_conversation_data'
    s.Execute(sql)


def mms_parsing(message_xml):
    date = message_xml["@date"]
    for part in message_xml["parts"].values():
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
    return date, author, text


def sms_parsing(message_xml):
    author = 'Rebecca' # I don't actually know how to determine the author for sms message types
    date = message_xml["@date"]
    text = message_xml["@body"]
    return author, date, text


def read_xml(source_file_name):
    print(f'{source_file_name}: reading file data...')
    with open(source_file_name, 'r', encoding='utf-8') as f:
        my_xml = f.read()
    # print(f'read {len(my_xml)} thingies')
    return my_xml


def main():

    def do_append(unix_timestamp, author, text):
        local_time = datetime.fromtimestamp(float(unix_timestamp)/1000, local_timezone)
        all_data.append([unix_timestamp, local_time, author, text])

    for source_file_name in get_file_names_from_repository():
        all_data = []
        my_xml = read_xml(source_file_name)
        s0 = xmltodict.parse(my_xml)
        s1 = s0['smses']

        print('parsing content...')
        for k,v in s1.items():
            if k == 'mms':
                counter = 0
                for vv in v:
                    date, author, text = mms_parsing(vv)
                    do_append(date, author, text)
            elif k == 'sms':
                for vv in v:
                    date, author, text = sms_parsing(vv)
                try:
                    do_append(date, author, text)
                except Exception as e:
                    print(f'\n\nUnhandled exception {e}\ndata:\n{k}\n{v}')
                    exit()
            elif type(v) == 'str':
                pass
            else:
                print(f'ignored content key {k}')
        print(f'{len(all_data)} records read\n')
        print('writing to database...')
        write_to_db(all_data)
        load_db()
        # remove(source_file_name)
    print('done')

if __name__ == '__main__':
    main()
