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


def write_to_db(text_data:list, MIME_data:list):
    s = clsSQLServer.Interface(database='Convo')
    d = datetime.now(local_timezone)
    for i in text_data:
        i.append(d)
    text_sql = \
"""insert into Convo.dbo.load_conv(source_timestamp, converted_timestamp, author, body, record_created) 
values (?, ?, ?, ?, ?);"""
    s.InsertMany(text_sql, text_data)
    MIME_sql = \
"""insert into Convo.dbo.load_multimedia(mms_timestamp, mms_author, part_ct, part_cl, part_data)
values (?, ?, ?, ?, ?);"""
    print(MIME_data[0])
    s.InsertMany(MIME_sql, MIME_data)
    pass


def load_db():
    s = clsSQLServer.Interface(database='Convo')
    sql = 'exec dbo.load_conversation_data'
    s.Execute(sql)


def mms_parsing(message_xml):
    message = {}
    message['date'] = message_xml["@date"]
    for part in message_xml["parts"].values():
        message['author'] = 'Rebecca'
        if isinstance(part, list):
            for od in part:
                seq_flag = int(od['@seq'])
                if seq_flag == -1:
                    message['author'] = 'Andy'
                else:
                    message['text'] = od["@text"]
            message['MIME'] = False
        elif isinstance(part, dict):
            if part["@seq"] == '-1':
                message['author'] = 'Andy'
            message['text'] = part["@text"]
            message['ct'] = part['@ct']
            message['cl'] = part['@cl']
            try:
                message['data'] = part['@data']
                message['MIME'] = True
            except KeyError:
                message['MIME'] = False
        else:
            message['author'] = '<unknown>'
            message['text'] = '<unknown>'
    return message


def sms_parsing(message_xml):
    message = {}
    message['author'] = 'Rebecca' # I don't actually know how to determine the author for sms message types
    message['date'] = message_xml["@date"]
    message['text'] = message_xml["@body"]
    message['MIME'] = False
    return message


def read_xml(source_file_name):
    print(f'{source_file_name}: reading file data...')
    with open(source_file_name, 'r', encoding='utf-8') as f:
        my_xml = f.read()
    # print(f'read {len(my_xml)} thingies')
    return my_xml


def main():

    def do_append(message):
        local_time = datetime.fromtimestamp(float(message['date'])/1000, local_timezone)
        if message['MIME']:
            MIME_data.append([message['date'], message['author'], message['ct'], message['cl'], message['data']])
        else:
            all_data.append([message['date'], local_time, message['author'], message['text']])

    # for source_file_name in get_file_names_from_repository():
    for source_file_name in [r"H:\OneDrive\Apps\SMS Backup and Restore\done\sms-20250321031031.xml"]:
        all_data = []
        MIME_data = []
        my_xml = read_xml(source_file_name)
        s0 = xmltodict.parse(my_xml)
        s1 = s0['smses']

        print('parsing content...')
        for k,v in s1.items():
            if k == 'mms':
                counter = 0
                for vv in v:
                    message = mms_parsing(vv)
                    do_append(message)
            elif k == 'sms':
                for vv in v:
                    message = sms_parsing(vv)
                do_append(message)
            elif type(v) == 'str':
                pass
            else:
                pass
                # print(f'ignored content key {k}')
        print(f'{len(all_data)} records read\n')
        print('writing to database...')
        write_to_db(all_data, MIME_data)
        load_db()
        # remove(source_file_name)
    print('done')

if __name__ == '__main__':
    main()
