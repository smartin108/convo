import xmltodict
import pprint
import json
import clsSQLServer


def write_to_db(chonk:list):
    s = clsSQLServer.Interface(database='Convo')
    sql = \
"""insert into Convo.dbo.conv(source_timestamp, author, body) 
values (?, ?, ?);"""
    s.InsertMany(sql, chonk)


def main():
    source_file = './sms-20241206001051.xml'
    all_data = []

    with open(source_file, 'r', encoding='utf-8') as f:
        my_xml = f.read()


    s0 = xmltodict.parse(my_xml)
    s1 = s0['smses']

    for k,v in s1.items():
        if k == 'mms':
            counter = 0
            # print(f'  {k}[{counter}]')
            for vv in v:
                date = vv["@date"]
                # print(f'    date: {vv["@date"]}')
                for part in vv["parts"].values():
                    author = 'Rebecca'
                    if isinstance(part, list):
                        for od in part:
                            seq_flag = int(od['@seq'])
                            if seq_flag == -1:
                                author = 'Andy'
                            else:
                                # print(f'      author={author}')
                                text = od["@text"]
                                # print(f'      text={od["@text"]}')
                    elif isinstance(part, dict):
                        if part["@seq"] == '-1':
                            author = 'Andy'
                        # print(f'      author={author}')
                        text = part["@text"]
                        # print(f'      text={m_text}')
                    else:
                        # print(f'          unknown type {type(part)}')
                        # print(f'          content={part}')
                        author = '<unknown>'
                        text = '<unknown>'
                # print()
                # counter += 1
                # if counter > 100:
                #     break
                all_data.append([date, author, text])
        elif k == 'sms':
            # print(f'  {k}')
            for vv in v:
                author = 'Rebecca' # I don't actually know how to setermine the author for sms message types
                date = vv["@date"]
                # print(f'    address: {vv["@address"]}')
                text = vv["@body"]
                # print(f'    date: {vv["@date"]}')
                # print(f'    address: {vv["@address"]}')
                # print(f'    body: {vv["@body"]}')
                # print()
            all_data.append([date, author, text])
        elif type(v) == 'str':
            pass
            # print(f'{k}: {v}')
        else:
            pass
            # print(f'{k}: ({type(v)}) v: ')
    for i in all_data:
        print(i)
    write_to_db(all_data)

if __name__ == '__main__':
    main()
