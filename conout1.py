import clsSQLServer
import datetime

S = clsSQLServer.Interface(database='Convo')

q = """select converted_timestamp, author, body from Convo.dbo.conv order by converted_timestamp"""

data = S.SelectQuery(q)

outfile = './out.html'



def headers():
    return \
"""
<!DOCTYPE html>
<html lang="en">
<head><link rel="stylesheet" href="out.css"></head>
<body>
    <table class="center">
"""


def trailers():
    return \
"""
    </table>
</body>
</html>

"""


def make_format(body, align, t_b):
    return '    <tr><td class="%s" border-top-width: %s><p>%s</p></td></tr>\n'%(align, t_b, body)

previous_author = 'None'
previous_timestamp = datetime.datetime.now()
with open(outfile, 'w', encoding='UTF-8') as f:
    f.write(headers())
    for r in data:
        timestamp = r[0]
        author = r[1]
        body = r[2].replace('\n','<br>')
        if author == 'Rebecca':
            align = 'left'
        elif author == 'Andy':
            align = 'right'
        else:
            print(f'unknown author in record:\n{r}')
        if author == previous_author:
            t_b = '1 px dotted'
        else:
            t_b = '1 px solid'
        if (timestamp - previous_timestamp).seconds > 3600:
            text = make_format(datetime.datetime.strftime(timestamp, '%A') + ', ' + timestamp.isoformat(), align, t_b)
            f.write(text)
        text = make_format(body, align, t_b)
        f.write(text)
        previous_timestamp = timestamp
        previous_author = author
    f.write(trailers())