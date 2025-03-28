import xmltodict
import pprint
import json
import clsSQLServer
import tzlocal
from datetime import datetime
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from os import walk
from os import remove
from sys import exit
# from hashlib import sha256
from uuid import uuid4
import clsLogger
import pprint


logger = clsLogger.Interface(filename='conparse.log', logname='conparse', level='WARNING')


def local_timezone():
    return tzlocal.get_localzone()


def do_pause():
    _ = input('paused...')
    return 0


def get_file_names_from_repository():
    """
    Iterator returns all the backup files from the default location
    """
    backup_location = "H:/OneDrive/Apps/SMS Backup and Restore"
    for subdir, _, files in walk(backup_location):
        for filename in files:
            yield f'{subdir}/{filename}'


def user_get_file():
    Tk().withdraw()
    source_file = askopenfilename(initialdir='C://Users//Andy//Downloads')
    return source_file


def chunkify(big_text:str, chunk_size:int=4000):
    """
    break up the big_text input into smaller pieces
    """
    if big_text:
        N = (len(big_text) - 1) // chunk_size + 1
        j = list((chunk_size * i) for i in range(N))
        j.append(len(big_text))
        for i in range(N-1):
            yield i, big_text[j[i]:j[i+1]] # chunk#, chunk_data
    else:
        return 0, ''


def write_MIME_to_db(MIME_messages):
    s = clsSQLServer.Interface(database='Convo')
    d = datetime.now(local_timezone())
    MIME_sql = \
        """ insert into land.multimedia (
                message_uuid, 
                chunk_number, 
                chunk_data, 
                record_created)
            values (?, ?, ?, ?); """
    for r in MIME_messages:
        r.append(d)
    s.InsertMany(MIME_sql, MIME_messages)
    return 0


def write_to_db(text_messages:list, MIME_messages:list=None):
    s = clsSQLServer.Interface(database='Convo')
    d = datetime.now(local_timezone())
    for i in text_messages:
        i.append(d)
    text_sql = \
        """ insert into land.text_message (
                source_timestamp, 
                converted_timestamp, 
                sequence_number,
                message_uuid, 
                author, 
                body, 
                ct,
                cl,
                mms_m_size,
                mms_tr_id,
                mms_id,
                record_created) 
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """
    s.InsertMany(text_sql, text_messages)

    if MIME_messages:
        write_MIME_to_db(MIME_messages)
    return 0


def load_db():
    s = clsSQLServer.Interface(database='Convo')
    sql = 'exec dev.do_load;'
    s.Execute(sql)
    return 0


def read_xml(source_file_name):
    with open(source_file_name, 'r', encoding='utf-8') as f:
        my_xml = f.read()
    return my_xml


def do_append_text(text_messages, new_message):
    """
    This output of this functions is a recordset that will be uploaded
    to the database. Order is important!
    """
    local_time = datetime.fromtimestamp(float(new_message['date'])/1000, local_timezone())
    text_messages.append([\
        new_message['date'], 
        local_time, 
        new_message['sequence_number'],
        new_message['uuid'],
        new_message['author'], 
        new_message['text'],
        new_message['ct'],
        new_message['cl'],
        new_message['mms_m_size'],
        new_message['mms_tr_id'],
        new_message['mms_id']
        ])
    return text_messages


def do_append_MIME(MIME_messages, new_MIME_message):
    """
    This output of this functions is a recordset that will be uploaded
    to the database. Order is important!
    """
    for chunk in new_MIME_message:
        MIME_messages.append([\
            chunk['uuid'],
            chunk['chunk_number'],
            chunk['chunk_data']
            ])
    return MIME_messages


def extract_MIME_data(uuid, message_data):
    MIME_message = []
    for chunk_number, chunk_data in chunkify(message_data):
        MIME_content = {}
        MIME_content['uuid'] = uuid
        MIME_content['chunk_number'] = chunk_number
        MIME_content['chunk_data'] = chunk_data
        MIME_message.append(MIME_content)
    return MIME_message


def mms_part_data(part, text_message_dict:dict):
    text_message_dict['sequence_number'] = int(part['@seq'])
    if part["@seq"] == '-1':
        text_message_dict['author'] = 'Andy'
    text_message_dict['text'] = part["@text"]
    text_message_dict['ct'] = part['@ct']
    text_message_dict['cl'] = part['@cl']

    try:
        message_data = part['@data']
    except KeyError:
        MIME_message = []
    else:
        u = uuid4()
        MIME_message = extract_MIME_data(u, message_data)
        text_message_dict['uuid'] = u
    return text_message_dict, MIME_message



def mms_parsing(dict_level_2):
    print(dict_level_2)
    exit()
    for part in dict_level_2["parts"].values():
        text_message_dict = {} # single message as dict
        text_message_dict['date'] = dict_level_2["@date"]
        text_message_dict['author'] = 'Rebecca'
        text_message_dict['uuid'] = None
        text_message_dict['mms_m_size'] = dict_level_2['@m_size']
        text_message_dict['mms_tr_id'] = dict_level_2['@tr_id']
        text_message_dict['mms_id'] = dict_level_2['@_id']

        if isinstance(part, list):
            for mini_part in part:
                text_message_dict, MIME_message = mms_part_data(mini_part, text_message_dict)

        elif isinstance(part, dict):
            text_message_dict, MIME_message = mms_part_data(part, text_message_dict)

        else:
            text_message_dict['author'] = '<unknown>'
            text_message_dict['text'] = '<unknown>'
    return text_message_dict, MIME_message


def sms_parsing(message_xml):
    message = {}
    message['sequence_number'] = 0
    message['author'] = 'Rebecca' # I don't actually know how to determine the author for sms message types
    message['date'] = message_xml["@date"]
    message['text'] = message_xml["@body"]
    message['uuid'] = None
    message['ct'] = None
    message['cl'] = None
    message['mms_m_size'] = None
    message['mms_tr_id'] = None
    message['mms_id'] = None
    return message


def do_content_loop(messages_as_dict):
    text_messages = [] # list of lists [[message1],[message2][...]]
    MIME_messages = [] # list of lists of lists [[message1[chunk1],[chunk2],[...]],message2[[chunk1],[chunk2],[...]]]]
    for message_type, dict_level_1 in messages_as_dict.items():
        if message_type == 'mms':
            for dict_level_2 in dict_level_1:
                new_message, new_MIME_message = mms_parsing(dict_level_2)
                do_append_text(text_messages, new_message)
                if new_MIME_message:
                    do_append_MIME(MIME_messages, new_MIME_message)
        elif message_type == 'sms':
            for dict_level_2 in dict_level_1:
                new_message = sms_parsing(dict_level_2)
            do_append_text(text_messages, new_message)
        elif message_type in ['@count', '@backup_set', '@backup_date', '@type']:
            pass
        else:
            raise TypeError(f'Unknown message_type "{message_type}"')
    return text_messages, MIME_messages


def do_input_file_work(source_file_name):
    """
    read one input file and make it xml
    if <sampling> is specified, return approximately <sampling>/100 total items
    """
    print(f'reading {source_file_name}...')
    file_xml = read_xml(source_file_name)
    xml_as_dict = xmltodict.parse(file_xml)
    messages_as_dict = xml_as_dict['smses']
    return messages_as_dict


def main():
    # for source_file_name in get_file_names_from_repository():
    for source_file_name in [r"H:\OneDrive\Apps\SMS Backup and Restore\done\sms-20250321031031.xml"]:
        messages_as_dict = do_input_file_work(source_file_name)
        text_messages, MIME_messages = do_content_loop(messages_as_dict)

        print(f'{len(text_messages)} records read\n')
        print('writing to database...')
        write_to_db(text_messages, MIME_messages)
        load_db()
        # remove(source_file_name)
    print('done')


if __name__ == '__main__':
    main()
