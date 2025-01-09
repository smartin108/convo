import xmltodict
import pprint
import json

source_file = './sms-20241206001051.xml'

with open(source_file, 'r', encoding='utf-8') as f:
    my_xml = f.read()


s0 = xmltodict.parse(my_xml)
s1 = s0['smses']

for k,v in s1.items():
    print(f'    {k}: {v}')
    """
    if k == 'sms':
        print(k)
        for m,n in v.items():
            print(f'    {m}: {n}')
    """
