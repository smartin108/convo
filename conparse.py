import xmltodict
import pprint
import json
import clsSQLServer
from datetime import datetime
import tzlocal


def write_to_db(chonk:list):
    s = clsSQLServer.Interface(database='Convo')
    sql = \
"""insert into Convo.dbo.conv(source_timestamp, converted_timestamp, author, body) 
values (?, ?, ?, ?);"""
    s.InsertMany(sql, chonk)


def main():

    def do_append(unix_timestamp, author, text):
        local_timezone = tzlocal.get_localzone()
        local_time = datetime.fromtimestamp(float(unix_timestamp)/1000, local_timezone)
        all_data.append([unix_timestamp, local_time, author, text])

    source_file = './sms-20250112101206.xml'
    all_data = []

    print('reading file data...')
    with open(source_file, 'r', encoding='utf-8') as f:
        my_xml = f.read()


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
    for i in all_data:
        print(i)
    print('\n\nwriting to database...')
    write_to_db(all_data)

if __name__ == '__main__':
    main()
