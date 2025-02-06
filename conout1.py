import clsSQLServer
import datetime

S = clsSQLServer.Interface(database='Convo')

q = """select converted_timestamp, author, body from Convo.dbo.conv order by converted_timestamp"""

data = S.SelectQuery(q)

outfile = './out.html'



def headers():
    return \
"""<!DOCTYPE html>
<html lang="en">
<head><link rel="stylesheet" href="out.css"></head>
<body>
    <table class="center">
"""


def trailers():
    return \
"""    </table>
</body>
</html>

"""


def cell_body(body, timestamp, author, cell_class=None):
    return \
"""        <tr>
            <td class="%s">
                <div class="tsdiv">
                    <p>
                        %s
                    </p>
                </div>
                <div class="tshide">
                    %s %s
                </div>
            </td>
        </tr>\n"""%(cell_class, body, author, timestamp)


previous_author = 'None'
previous_timestamp = datetime.datetime.now()
breaker = 10000
counter = 0
with open(outfile, 'w', encoding='UTF-8') as f:
    f.write(headers())
    for r in data:
        counter += 1
        timestamp = r[0]
        author = r[1]
        body = r[2].replace('\n','<br>')
        if (timestamp - previous_timestamp).seconds > 3600:
            text = cell_body(datetime.datetime.strftime(timestamp, '%A') + ', ' + timestamp.isoformat(), timestamp, '', 'timestamp')
            f.write(text)

        if author == 'Rebecca' and previous_author == 'Rebecca':
            cell_class = 'cont left'
        elif author == 'Rebecca' and previous_author != 'Rebecca':
            cell_class = 'sect left'
        elif author == 'Andy' and previous_author == 'Andy':
            cell_class = 'cont right'
        elif author == 'Andy' and previous_author != 'Andy':
            cell_class = 'sect right'
        else:
            print(f'unknown author in record:\n{r}')
        text = cell_body(body, timestamp.isoformat(), author, cell_class)
        f.write(text)
        previous_timestamp = timestamp
        previous_author = author
        if counter >= breaker:
            break
    f.write(trailers())
print(f'{counter} rows out')