'''
Created on 2019年7月17日

@author: user
'''
import pandas as pd

df = pd.read_csv('10100119.csv')

def conversion(row):
    xml = ['<item>']
    for f in row.index:
        xml.append('  <"{0}">{1}</field>'.format(f, row[f]))
    xml.append('</item>')
    xml = '\n'.join(xml)
    return xml

result = []
for i,r in df.tail().iterrows():
    result.append(conversion(r))

print ('\n'.join(result))