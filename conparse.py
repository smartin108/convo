import xmltodict
import pprint
import json

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
            print(f'    address: {vv["@address"]}')
            print(f'    parts: {str(vv["parts"])[:100]}')
            for part in vv["parts"].values():
                print(f'      part: {type(part)}  {str(part)[:100]}')
                # if part['@seq'] == '0':
                #     print(f'      {part["@text"]}')
            print()
            counter += 1
            if counter > 10:
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