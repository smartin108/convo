import xmltodict
import pprint
import json
import clsSQLServer
import tzlocal
from datetime import datetime
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from os import walk
# from os import listdir
from os import path
from os import remove
from sys import exit
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
            if 'done' not in subdir:
                yield path.join(subdir, filename)


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
                part_uuid, 
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
        f""" insert into land.text_message (
                message_service,
                message_date, 
                message_timestamp, 
                message_author,
                mms_m_size,
                mms_tr_id,
                mms_id,
                part_uuid, 
                part_seq,
                part_ct,
                part_cl,
                part_text, 
                record_created) 
            values ({','.join('?'*13)}); """
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
    local_time = datetime.fromtimestamp(float(new_message['message_date'])/1000, local_timezone())
    try:
        text_messages.append([\
            new_message['message_service'], 
            new_message['message_date'], 
            local_time, 
            new_message['message_author'], 
            new_message['mms_m_size'],
            new_message['mms_tr_id'],
            new_message['mms_id'],
            new_message['part_uuid'],
            new_message['part_seq'],
            new_message['part_ct'],
            new_message['part_cl'],
            new_message['part_text']
            ])
    except KeyError as e:
        print(e)
        print(new_message)
        exit()
    return text_messages


def do_append_MIME(MIME_messages, new_MIME_message):
    """
    This output of this functions is a recordset that will be uploaded
    to the database. Order is important!
    """
    for chunk in new_MIME_message:
        MIME_messages.append([\
            chunk['part_uuid'],
            chunk['chunk_number'],
            chunk['chunk_data']
            ])
    return MIME_messages


def extract_MIME_data(uuid, message_data):
    MIME_message = []
    for chunk_number, chunk_data in chunkify(message_data):
        MIME_content = {}
        MIME_content['part_uuid'] = uuid
        MIME_content['chunk_number'] = chunk_number
        MIME_content['chunk_data'] = chunk_data
        MIME_message.append(MIME_content)
    return MIME_message


def mms_part_data(part, message:dict):
    message['part_seq'] = part['@seq']
    if message['part_seq'] == '-1':
        message['message_author'] = 'Andy'
    message['part_ct'] = part['@ct']
    message['part_cl'] = part['@cl']
    message['part_text'] = part["@text"]

    try:
        message_data = part['@data']
    except KeyError:
        MIME_message = []
    else:
        u = uuid4()
        MIME_message = extract_MIME_data(u, message_data)
        message['part_uuid'] = u
    return message, MIME_message



def mms_parsing(extracted_xml, part):
    message = {} 
    MIME_message = []
    message['message_service'] = 'mms'
    message['message_date'] = extracted_xml["@date"]
    message['message_author'] = 'Rebecca'
    message['mms_m_size'] = extracted_xml['@m_size']
    message['mms_tr_id'] = extracted_xml['@tr_id']
    message['mms_id'] = extracted_xml['@_id']
    message['part_uuid'] = None

    if isinstance(part, list):
        for mini_part in part:
            message, MIME_message = mms_part_data(mini_part, message)
            yield message, MIME_message

    elif isinstance(part, dict):
        message, MIME_message = mms_part_data(part, message)
        yield message, MIME_message

    else:
        print(part.type())
        message['message_author'] = 'unknown'
        message['part_text'] = 'unknown'
        yield message, MIME_message


def sms_parsing(extracted_xml):
    message = {}
    message['message_service'] = 'sms'
    message['message_date'] = extracted_xml["@date"]
    message['message_author'] = 'unknown'
    message['mms_m_size'] = None
    message['mms_tr_id'] = None
    message['mms_id'] = None
    message['part_uuid'] = None
    message['part_seq'] = None
    message['part_ct'] = None
    message['part_cl'] = None
    message['part_text'] = extracted_xml["@body"]
    return message


def do_content_loop(messages_as_dict):
    text_messages = [] # list of lists [[message1],[message2][...]]
    MIME_messages = [] # list of lists of lists [[message1[chunk1],[chunk2],[...]],message2[[chunk1],[chunk2],[...]]]]
    for message_type, dict_level_1 in messages_as_dict.items():
        if message_type == 'mms':
            for extracted_xml in dict_level_1:
                for part in extracted_xml['parts'].values():
                    for new_message, new_MIME_message in mms_parsing(extracted_xml, part):
                        do_append_text(text_messages, new_message)
                        if new_MIME_message:
                            do_append_MIME(MIME_messages, new_MIME_message)
        elif message_type == 'sms':
            for extracted_xml in dict_level_1:
                new_message = sms_parsing(extracted_xml)
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
    print(f'\nreading {source_file_name}')
    file_xml = read_xml(source_file_name)
    xml_as_dict = xmltodict.parse(file_xml)
    messages_as_dict = xml_as_dict['smses']
    return messages_as_dict


def main():
    for source_file_name in get_file_names_from_repository():
    # for source_file_name in [r"H:\OneDrive\Apps\SMS Backup and Restore\done\sms-20250321031031.xml"]:
        messages_as_dict = do_input_file_work(source_file_name)
        text_messages, MIME_messages = do_content_loop(messages_as_dict)

        print(f'{len(text_messages)} records read')
        print('writing to database...')
        write_to_db(text_messages, MIME_messages)
        load_db()
        # remove(source_file_name)
    print('done')


if __name__ == '__main__':
    main()
