import clsSQLServer

S = clsSQLServer.Interface(database='Convo')

q = """select converted_timestamp, author, body from Convo.dbo.conv order by converted_timestamp"""

data = S.SelectQuery(q)

# print(data)


def headers(frame_width):
    return \
"""
#!/usr/bin/env python3

print(\"\"\"
Content-type: text.html

<!DOCTYPE html>
<html lang="en">
<head>
    <link rel="stylesheet" href="out.css">
</head>
<body>
    <table><tr><th width="400px"></th></tr>
"""


def trailers():
    return '</table></body></html>\n\"\"\")'


def make_format(frame_width, body, align):

    def sanitize(t):
        temp = t
        replacements = [\
            ['“','"'],\
            ['”','"'],\
            [' ',' '],\
            ['​',' '],\
            ['🇭🇺',' '],\
            ['\U0001f61d',':P'],\
            ['😋',':P'],\
            ['😛',':P'],\
            ['🤷','\\/'],\
            ['😊',':)'],\
            ['👍',' '],\
            ['🎉',' '],\
            ['🥰',' '],\
            ['💞',' '],\
            ['😁',':)'],\
            ['🤣',':)'],\
            ['😂',':)'],\
            ['🙂',':)'],\
            ['😅',':)'],\
            ['😘',''],\
            ['💋',''],\
            ['😭',''],\
            ['🤪',''],\
            ['☺️',':)'],\
            ['😈',':)'],\
            ['❤️','<3'],\
            ['❤','<3'],\
            ['♥️','<3'],\
            ['🤍','<3']\
            ]
        for r in replacements:
            temp = temp.replace(r[0], r[1])
        return temp

    return '    <tr><td width="400px">%s</td></tr>\n'%(sanitize(body))
    # return '    <tr><td width="400px"><p>%s</p></td></tr>\n'%(body)


frame_width = '50%'
outfile = './cgi-bin/hello.py'
with open(outfile, 'w', encoding='UTF-8') as f:
    f.write(headers(frame_width))
    for r in data:
        if r[1] == 'Rebecca':
            align = 'left'
        elif r[1] == 'Andy':
            align = 'right'
        else:
            print(f'unknown author in record:\n{r}')
        text = make_format(frame_width, r[2], align)
        f.write(text)
    f.write(trailers())