__author__ = 'Anirudh'

from bs4 import BeautifulSoup
from os import listdir
from os.path import isfile, join
import re
import xml,sax



# Extracting the file names from the smaller dataset folder into a list

mypath='small_pages'
onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]

# Removing the DS_STORE file which gets created

onlyfiles.pop(0)

print 'No of files is :', len(onlyfiles)

page_dict={}

def text_parsing2(file):


    links = []
    with open(file) as f:
        soup=BeautifulSoup(f)

    text = soup.findAll(text=True);
    title = [a.findAll(text=True) for a in soup.findAll('title')]

    print type(title)

    title_key = ''.join(title)

    print 'title key is:', title_key

    for a in soup.findAll('a'):
        links.append(a.get('href'))

    #page_dict[title]=links


    return page_dict[title_key]



def text_parsing(md):


    LINK_RE = re.compile(r'\[\[([^\]]+)\]')
    TEXT_RE = re.compile(r'<.+>\s*(.+)\s*<.+>')



    #find_md_links(body_markdown)

    print 'Regex Output:',TEXT_RE.findall(md)

    return LINK_RE.findall(md),TEXT_RE.findall(md)


with open('small_pages/page-0001000.xml') as f:
    for each in f:
        print f
        x,y = text_parsing(str(f))

        print "output is :", x, y