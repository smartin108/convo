import xmltodict
import pprint
import json


def message_parts(message:str):
    pass


def main():
    source_file = './sms-20241206001051.xml'

    with open(source_file, 'r', encoding='utf-8') as f:
        my_xml = f.read()


    s0 = xmltodict.parse(my_xml)
    s1 = s0['smses']

    for k,v in s1.items():
        if type(v) == 'str':
            print(f'{k}: {v}')
        elif k == 'mms':
            counter = 0
            print(f'  {k}[{counter}]')
            for vv in v:
                print(f'    date: {vv["@date"]}')
                # print(f'    address: {vv["@address"]}')
                # print(f'    parts: {str(vv["parts"])[:100]}')
                for part in vv["parts"].values():
                    author = 'Rebecca'
                    if isinstance(part, list):
                        # print(f'      part: {str(part)[:100]}')
                        for od in part:
                            seq_flag = int(od['@seq'])
                            if seq_flag == -1:
                                author = 'Andy'
                            else:
                            # if int(od['@seq']) > -1:
                                # print(f'      part seq={od["""@seq"""]}: {od}')
                                print(f'      author={author}')
                                print(f'      text={od["@text"]}')
                    elif isinstance(part, dict):
                        # print(f'          {part}')
                        if part["@seq"] == '-1':
                            author = 'Andy'
                        print(f'      author={author}')
                        m_text = part["@text"]
                        data_only = False
                        if m_text == 'null':
                            if part["@data"]:
                                data_only = True
                                m_text = '<data only>'
                        print(f'      text={m_text}')
                    else:
                        print(f'          unknown type {type(part)}')
                        print(f'          content={part}')
                print()
                counter += 1
                if counter > 100000:
                    break
        elif k == 'sms':
            print(f'  {k}')
            for vv in v:
                print(f'    date: {vv["@date"]}')
                print(f'    address: {vv["@address"]}')
                print(f'    body: {vv["@body"]}')
                print()
        else:
            print(f'{k}: ({type(v)}) v: ')

if __name__ == '__main__':
    main()
