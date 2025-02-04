import clsSQLServer

S = clsSQLServer.Interface(database='Convo')

q = """select converted_timestamp, author, body from Convo.dbo.conv order by converted_timestamp"""

data = S.SelectQuery(q)

# print(data)


def headers(frame_width):
    return \
"""<html>
<head>
    <link rel="stylesheet" href="out.css">
</head>
<body>
    <table><tr><th width="400px"></th></tr>
"""


def trailers():
    return '</table></body></html>\n'


def make_format(frame_width, body, align):
    return '    <tr><td width="400px">%s</td></tr>\n'%(body)
    # return '    <tr><td width="400px"><p>%s</p></td></tr>\n'%(body)


frame_width = '50%'
outfile = './out.html'
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