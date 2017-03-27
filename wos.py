from __future__ import print_function
import requests
from bs4 import BeautifulSoup
import re
import time
import psycopg2
import random
import pdb
import os
import sys

def list_to_pair(lst):
    return [lst[i:i+2] for i in range(0, len(lst), 2)]

def long_sleep(sleeptime):
    starttime = time.time()
    while time.time() - starttime < sleeptime:
        print("waiting, %i seconds         \r"%int(starttime + sleeptime - time.time()), end='')
        sys.stdout.flush()
        time.sleep(0.1)
    print('\n')

def scrap_games_index():
    print("collecting games index page...")
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS games_index (
                id			SERIAL UNIQUE PRIMARY KEY,
                link		VARCHAR(1024) NOT NULL,
                complete	BOOL DEFAULT FALSE
            );
            """)
    except Exception as e:
        print(e)
    conn.commit()

    try:
        rnd_num = random.randint(0,2)
        req = sessions[rnd_num].get(STARTING_PAGE, headers=headers[rnd_num])
        bsObj = BeautifulSoup(req.text, "html.parser")
    except e as e:
        print(e)
        return None

    for link in bsObj.findAll("a",href=re.compile("^.\.html")):
        try:
            cur.execute("INSERT INTO games_index (link) VALUES (%s)", (link.get('href'),))
        except Exception as e:
            print(e)
            return None

    conn.commit()

def scrap_games_pages():
    print("collecting games pages...")
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS games_pages (
                id			SERIAL UNIQUE PRIMARY KEY,
                link		VARCHAR(1024) NOT NULL,
                complete	BOOL DEFAULT FALSE
            );
            """)
    except Exception as e:
        print(e)
    conn.commit()

    try:
        cur.execute("SELECT * FROM games_index WHERE complete = FALSE")
    except Exception as e:
        print(e)
        return None

    rows = cur.fetchall()
    for row in rows:
        print("open %s"%row[1])
        try:
            rnd_num = random.randint(0,2)
            req = sessions[rnd_num].get(GAMES_URL + row[1], headers=headers[rnd_num])
            bsObj = BeautifulSoup(req.text, "html.parser")
        except AttributeError as e:
            print(e)
            return None

        for link in bsObj.findAll("a",href=re.compile("^/infoseekid*")):
            try:
                cur.execute("INSERT INTO games_pages (link) VALUES (%s)", (link.get('href')[1:],))
            except Exception as e:
                print(e)
                return None

        try:
            cur.execute("UPDATE games_index SET complete = TRUE WHERE id = %s",(row[0],))
        except Exception as e:
            print(e)
            return None

        conn.commit()
        time.sleep(1)

def scrap_games():
    print("collecting games...")

    # load database table structure
    try:
        cur.execute("SELECT * FROM information_schema.tables WHERE table_name='games_struct'")
        if not bool(cur.rowcount): #if not exist - initializing struct table
            games_struct = (
                ("title","VARCHAR(1024)","Full title"), #NOT NULL
                ("aka","VARCHAR(1024)","Also known as"),
                ("f_title","VARCHAR(1024)","Feature title"),
                ("year","VARCHAR(1024)","Year of release"),
                ("publisher","VARCHAR(1024)","Publisher"),
                ("rereleased","VARCHAR(1024)","Re-released by"),
                ("modified","VARCHAR(1024)","Modified by"),
                ("author","VARCHAR(4096)","Author(s)"),
                ("orig_game","VARCHAR(1024)","Original game"),
                ("license","VARCHAR(1024)","License"),
                ("t_license","VARCHAR(1024)","Tie-in licence"),
                ("inspired","VARCHAR(1024)","Inspired by"),
                ("machine","VARCHAR(1024)","Machine type"),
                ("players","VARCHAR(1024)","Number of players"),
                ("controls","VARCHAR(1024)","Controls"),
                ("type","VARCHAR(1024)","Type"),
                ("language","VARCHAR(256)","Message language"),
                ("orig_publication","VARCHAR(1024)","Original publication"),
                ("orig_price","VARCHAR(1024)","Original price"),
                ("budget_price","VARCHAR(1024)","Budget price"),
                ("availability","VARCHAR(256)","Availability"),
                ("note","VARCHAR(1024)","Note"),
                ("protection","VARCHAR(1024)","Protection scheme"),
                ("authoring","VARCHAR(1024)","Authoring"),
                ("additional","VARCHAR(4096)","Additional info"),
                ("spot_comments","VARCHAR(1024)","SPOT comments"),
                ("series","VARCHAR(4096)","Series"),
                ("other_systems","VARCHAR(4096)","Other systems"),
                ("remarks","VARCHAR(4096)","Remarks"),
                ("score","VARCHAR(1024)","Score"))
            cur.execute("""
                CREATE TABLE games_struct (
                    id				SERIAL UNIQUE PRIMARY KEY,
                    field_name		VARCHAR(128) NOT NULL,
                    template		VARCHAR(1024) NOT NULL
                );
                """)
            for vals in games_struct:
                cur.execute("INSERT INTO games_struct (field_name,template) VALUES('%s')"%(vals[0] + "','" + vals[2]))


            query = "CREATE TABLE IF NOT EXISTS games (id SERIAL UNIQUE PRIMARY KEY," \
                        + "link VARCHAR(4096),"
            for field in games_struct:
                query = query + field[0] + " " + field[1] + (" NOT NULL" if field[1]=="title" else "") + ","
            try:
                cur.execute(query[:-1] + ")")
            except Exception as e:
                print(e)

            conn.commit()
    except Exception as e:
        print(e)

    try:
        cur.execute("SELECT * FROM games_struct")
    except Exception as e:
        print(e)
        return None

    struct = cur.fetchall()

    table = {} # create struct table and database table
    for field in struct:
        table[field[2]] = field[1]

    try: #prepare links list
        cur.execute("SELECT * FROM games_pages WHERE complete = FALSE")
    except Exception as e:
        print(e)
        return None

    rows = cur.fetchall() # rows - array of links to scrape
    for n,row in enumerate(rows):
        print("open %s"%row[1])

        for _ in range(2):	# try to get link, retry 3 times if error
            try:
                time.sleep(random.randint(1,3))
                rnd_num = random.randint(0,2)
                req = sessions[rnd_num].get(SITE_ROOT + row[1], headers=headers[rnd_num])
                bsObj = BeautifulSoup(req.text, "lxml")
                break
            except Exception as e:
                print(e)
        else:
            return None

        fields = ["link"]
        vals = ["'" + SITE_ROOT + row[1] + "'"]

        try: # look for custom fields on page
            for field in list_to_pair(bsObj.body.findAll("table")[5].findAll("td")): #load all pairs field-value from page
                if not(str(field[0].get_text()) in table): #new field found
                    table[str(field[0].get_text())] = re.sub(r"[^A-Za-z]+", '_', field[0].get_text()).lower()
                    cur.execute("INSERT INTO games_struct (field_name,template) VALUES('%s', '%s')"%(table[field[0].get_text()],field[0].get_text()))
                    cur.execute("ALTER TABLE games ADD COLUMN %s %s"%(table[field[0].get_text()],"VARCHAR(1024)" if len(field[1].get_text()) < 1024 else "VARCHAR(2048)"))
                fields.append(table[field[0].get_text()])
                vals.append("'" + field[1].get_text()[:-1].strip().replace("'","''") + "'")
        except Exception as e:
            print("Error processing new field! (%s)"%field[0].get_text())
            return None

        try: #send to database
            cur.execute("INSERT INTO games (%s) VALUES (%s)"%(','.join(fields),','.join(vals)))
        except Exception as e:
            print(e)
            print("INSERT INTO games (%s) VALUES (%s)"%(','.join(fields),','.join(vals)))
            return None

        try: #mark completed links
            cur.execute("UPDATE games_pages SET complete = TRUE WHERE id = %s",(row[0],))
        except Exception as e:
            print(e)
            return None

        conn.commit() # 1000 pages limit
        if n > 1000:
            print("1000 pages scraped, exiting...")
            return

def scrap_hardware_pages():
    print("collecting hardware pages...")
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hardware_pages (
                id			SERIAL UNIQUE PRIMARY KEY,
                link		VARCHAR(1024) NOT NULL,
                complete	BOOL DEFAULT FALSE
            );
            """)
    except Exception as e:
        print(e)
    conn.commit()

    try:
        rnd_num = random.randint(0,2)
        req = sessions[rnd_num].get(SITE_ROOT + "hw.html", headers=headers[rnd_num])
        bsObj = BeautifulSoup(req.text, "html.parser")
    except AttributeError as e:
        print(e)
        return None

    for link in bsObj.findAll("a",href=re.compile("^/infoseekid*")):
        try:
            cur.execute("INSERT INTO hardware_pages (link) VALUES (%s)", (link.get('href')[1:],))
        except Exception as e:
            print(e)
            return None

    conn.commit()
    time.sleep(1)

def scrap_hardware():
    print("collecting hardware...")

    # load database table structure
    try:
        cur.execute("SELECT * FROM information_schema.tables WHERE table_name='hardware_struct'")
        if not bool(cur.rowcount): #if not exist - initializing struct table
            hardware_struct = (
                ("device","VARCHAR(1024)","Device name"),) #NOT NULL
            cur.execute("""
                CREATE TABLE hardware_struct (
                    id				SERIAL UNIQUE PRIMARY KEY,
                    field_name		VARCHAR(128) NOT NULL,
                    template		VARCHAR(1024) NOT NULL
                );
                """)
            for vals in hardware_struct:
                cur.execute("INSERT INTO hardware_struct (field_name,template) VALUES('%s')"%(vals[0] + "','" + vals[2]))


            query = "CREATE TABLE IF NOT EXISTS hardware (id SERIAL UNIQUE PRIMARY KEY," \
                        + "link VARCHAR(4096),"
            for field in hardware_struct:
                query = query + field[0] + " " + field[1] + (" NOT NULL" if field[1]=="device" else "") + ","
            try:
                cur.execute(query[:-1] + ")")
            except Exception as e:
                print(e)

            conn.commit()
    except Exception as e:
        print(e)

    try:
        cur.execute("SELECT * FROM hardware_struct")
    except Exception as e:
        print(e)
        return None

    struct = cur.fetchall()

    table = {} # create struct table and database table
    for field in struct:
        table[field[2]] = field[1]

    try: #prepare links list
        cur.execute("SELECT * FROM hardware_pages WHERE complete = FALSE")
    except Exception as e:
        print(e)
        return None

    rows = cur.fetchall() # rows - array of links to scrape
    for n,row in enumerate(rows):
        print("open %s"%row[1])

        for _ in range(2):	# try to get link, retry 3 times if error
            try:
                time.sleep(random.randint(1,3))
                rnd_num = random.randint(0,2)
                req = sessions[rnd_num].get(SITE_ROOT + row[1], headers=headers[rnd_num])
                bsObj = BeautifulSoup(req.text, "lxml")
                break
            except Exception as e:
                print(e)
        else:
            return None

        fields = ["link"]
        vals = ["'" + SITE_ROOT + row[1] + "'"]

        try: # look for custom fields on page
            for field in list_to_pair(bsObj.body.findAll("table")[5].findAll("td")): #load all pairs field-value from page
                if not(str(field[0].get_text()) in table): #new field found
                    table[str(field[0].get_text())] = re.sub(r"[^A-Za-z]+", '_', field[0].get_text()).lower()
                    cur.execute("INSERT INTO hardware_struct (field_name,template) VALUES('%s', '%s')"%(table[field[0].get_text()],field[0].get_text()))
                    cur.execute("ALTER TABLE hardware ADD COLUMN %s %s"%(table[field[0].get_text()],"VARCHAR(1024)" if len(field[1].get_text()) < 1024 else "VARCHAR(2048)"))
                fields.append(table[field[0].get_text()])
                vals.append("'" + field[1].get_text()[:-1].strip().replace("'","''") + "'")
        except Exception as e:
            print("Error processing new field! (%s)"%field[0].get_text())
            return None

        try: #send to database
            cur.execute("INSERT INTO hardware (%s) VALUES (%s)"%(','.join(fields),','.join(vals)))
        except Exception as e:
            print(e)
            print("INSERT INTO hardware (%s) VALUES (%s)"%(','.join(fields),','.join(vals)))
            return None

        try: #mark completed links
            cur.execute("UPDATE hardware_pages SET complete = TRUE WHERE id = %s",(row[0],))
        except Exception as e:
            print(e)
            return None

        conn.commit() # 1000 pages limit
        if n > 1000:
            print("1000 pages scraped, exiting...")
            return

def scrap_game_files():
    print("collecting games's files...")

    # load database table structure
    try:
        cur.execute("SELECT * FROM information_schema.tables WHERE table_name='games_files_struct'")
        if not bool(cur.rowcount): #if not exist - initializing struct table
            games_files_struct = (
                ("filename","VARCHAR(1024)","Filename"), #NOT NULL,
                ("f_size","INTEGER","Size"),
                ("f_type","VARCHAR(1024)","Type"))
            cur.execute("""
                CREATE TABLE games_files_struct (
                    id				SERIAL UNIQUE PRIMARY KEY,
                    field_name		VARCHAR(128) NOT NULL,
                    template		VARCHAR(1024) NOT NULL
                );
                """)
            for vals in games_files_struct:
                cur.execute("INSERT INTO games_files_struct (field_name,template) VALUES('%s')"%(vals[0] + "','" + vals[2]))


            query = """CREATE TABLE IF NOT EXISTS games_files (
                            id SERIAL UNIQUE PRIMARY KEY,
                            link VARCHAR(1024) UNIQUE NOT NULL,
                            page_id INTEGER NOT NULL,
                            filetype VARCHAR(64) NOT NULL,
                    """
            for field in games_files_struct:
                query = query + field[0] + " " + field[1] + (" NOT NULL" if field[1]=="filename" else "") + ","
            try:
                cur.execute(query[:-1] + ")")
            except Exception as e:
                print(e)

            conn.commit()
    except Exception as e:
        print(e)

    try:
        cur.execute("SELECT * FROM games_files_struct")
    except Exception as e:
        print(e)
        return None

    struct = cur.fetchall()

    table = {} # create struct table and database table
    for field in struct:
        table[field[2]] = field[1]

    try: #prepare links list
        cur.execute("SELECT * FROM games_pages WHERE files_complete = FALSE")
    except Exception as e:
        print(e)
        return None

    rows = cur.fetchall() # rows - array of links to scrape
    for n,row in enumerate(rows):
        print("open %s"%row[1])

        page_id = row[0]
        for _ in range(4):	# try to get link, retry 5 times if error
            try:
                time.sleep(random.randint(1,3))
                rnd_num = random.randint(0,2)
                req = sessions[rnd_num].get(SITE_ROOT + row[1], headers=headers[rnd_num])
                bsObj = BeautifulSoup(req.text, "lxml")
                break
            except Exception as e:
                print(e)
        else:
            return None

        #Game files table
        searchtext = 'Download and play links'
        foundtext = bsObj.find('font',text=searchtext)

        if foundtext <> None:
            html_table = foundtext.findNext('table')
            t_rows = html_table.findChildren('tr')

            fields = ["link","page_id","filetype"]

            try: # checking table's header
                t_row = t_rows[0]
                cells = t_row.findChildren('td')
                for i in range(2,len(cells)):
                    field = str(re.sub(r"[\n]",'',cells[i].get_text()))
                    if not(field in table): #new field found
                        table[field] = re.sub(r"[^A-Za-z]+", '_', field).lower()
                        cur.execute("INSERT INTO games_files_struct (field_name,template) VALUES('%s', '%s')"%(table[field],field))
                        cur.execute("ALTER TABLE games_files ADD COLUMN %s %s"%(table[field],"VARCHAR(1024)" if len(field) < 1024 else "VARCHAR(2048)"))
                    fields.append(table[field])
            except Exception as e:
                print("Error processing new field! (%s)"%field)
                return None

            for t_row in t_rows[1:]: # collect data rows
                cells = t_row.findChildren('td')
                if len(cells) < 3:
                    continue
                if not cells[2].a['href'].count('/pub/sinclair'):
                    continue
                filelink = SITE_ROOT[:-1] + cells[2].a['href']
                vals = ["'" + filelink + "'","'" + str(page_id) + "'", "'game_file'"]

                for i in range(2,len(cells)):
                    c = re.sub(r"[\n']", '', cells[i].get_text())
                    c = (re.sub("[^0-9]", "", c) if i == 3 else c)
                    vals.append("NULL" if c=="" else "'" + c + "'" )

                for _ in range(len(fields)-len(vals)):
                    vals.append("NULL")

                for _ in range(4):	# try to download file, retry 5 times if error
                    if not os.path.isdir(FILES_PATH + re.sub(r"[\n\/]", '', cells[4].get_text())): # create directory
                        os.mkdir(FILES_PATH + re.sub(r"[\n\/]", '', cells[4].get_text()))
                    if cells[2].a['href'][-1] == '/': #directory
                        vals[2] = "'game_directory'"
                        print("game files directory found: %s"%cells[1].a['href'])
                        break
                    try:
                        print("downloading file %s"%cells[2].a['href'])
                        time.sleep(random.randint(1,3))
                        rnd_num = random.randint(0,2)
                        fileObj = sessions[rnd_num].get(filelink, headers=headers[rnd_num], stream=True)
                        if fileObj.status_code == 200:
                            with open(os.path.join(FILES_PATH, re.sub(r"[\n\/]", '', cells[4].get_text()), str(cells[2].get_text())), 'wb') as f:
                                for chunk in fileObj.iter_content(1024):
                                    f.write(chunk)
                                f.close()
                        break
                    except requests.exceptions.ConnectionError as nce:
                        print(nce)
                        if fileObj.status_code == "Connection refused":
                            long_sleep(120)
                    except Exception as e:
                        print(e)
                        print("n = %i"%_)
                else:
                    return None

                try: #send to database
                    cur.execute("INSERT INTO games_files (%s) VALUES (%s)"%(','.join(fields),','.join(vals)))
                except Exception as e:
                    print(e)
                    print("INSERT INTO games_files (%s) VALUES (%s)"%(','.join(fields),','.join(vals)))
                    return None


        #Additional files table
        searchtext = 'Additional material'
        foundtext = bsObj.find('font',text=searchtext)

        if foundtext <> None:
            html_table = foundtext.findNext('table')
            t_rows = html_table.findChildren('tr')

            fields = ["link","page_id","filetype"]

            try: # checking table's header
                t_row = t_rows[0]
                cells = t_row.findChildren('td')
                for i in range(1,len(cells)):
                    field = str(re.sub(r"[\n]",'',cells[i].get_text()))
                    if not(field in table): #new field found
                        table[field] = re.sub(r"[^A-Za-z]+", '_', field).lower()
                        cur.execute("INSERT INTO games_files_struct (field_name,template) VALUES('%s', '%s')"%(table[field],field))
                        cur.execute("ALTER TABLE games_files ADD COLUMN %s %s"%(table[field],"VARCHAR(1024)" if len(field) < 1024 else "VARCHAR(2048)"))
                    fields.append(table[field])
            except Exception as e:
                print("Error processing new field! (%s)"%field)
                return None

            for t_row in t_rows[1:]: # collect data rows
                cells = t_row.findChildren('td')
                if len(cells) < 3:
                    continue
                if not cells[1].a['href'].count('/pub/sinclair'):
                    continue
                filelink = SITE_ROOT[:-1] + cells[1].a['href']
                vals = ["'" + filelink + "'","'" + str(page_id) + "'", "'additional_file'"]

                for i in range(1,len(cells)):
                    c = re.sub(r"[\n']", '', cells[i].get_text())
                    c = (re.sub("[^0-9]", "", c) if i == 2 else c)
                    vals.append("NULL" if c=="" else "'" + c + "'" )

                for _ in range(len(fields)-len(vals)):
                    vals.append("NULL")

                #pdb.set_trace()
                for _ in range(4):	# try to download file, retry 5 times if error
                    if not os.path.isdir(ADDITIONAL_PATH + re.sub(r"[\n\/]", '', cells[3].get_text())): # create directory
                        os.mkdir(ADDITIONAL_PATH + re.sub(r"[\n\/]", '', cells[3].get_text()))
                    if cells[1].a['href'][-1] == '/': #directory
                        vals[2] = "'additional_directory'"
                        print("additional files directory found: %s"%cells[1].a['href'])
                        break
                    try:
                        print("downloading file %s"%cells[1].a['href'])
                        time.sleep(random.randint(1,3))
                        rnd_num = random.randint(0,2)
                        fileObj = sessions[rnd_num].get(filelink, headers=headers[rnd_num], stream=True)
                        if fileObj.status_code == 200:
                            with open(os.path.join(ADDITIONAL_PATH, re.sub(r"[\n\/]", '', cells[3].get_text()), str(cells[1].get_text())), 'wb') as f:
                                for chunk in fileObj.iter_content(1024):
                                    f.write(chunk)
                                f.close()
                        break
                    except requests.exceptions.ConnectionError as nce:
                        print(nce)
                        if fileObj.status_code == "Connection refused":
                            long_sleep(120)
                    except Exception as e:
                        print(e)
                        print("n = %i"%_)
                else:
                    return None

                try: #send to database
                    cur.execute("INSERT INTO games_files (%s) VALUES (%s)"%(','.join(fields),','.join(vals)))
                except Exception as e:
                    print(e)
                    print("INSERT INTO games_files (%s) VALUES (%s)"%(','.join(fields),','.join(vals)))
                    return None

        try: #mark completed links
            cur.execute("UPDATE games_pages SET files_complete = TRUE WHERE id = %s",(page_id,))
        except Exception as e:
            print(e)
            return None

        conn.commit() # 1000 pages limit
        if not n%1000 and n > 0:
            print("1000 pages scraped...")
            long_sleep(7200)


SITE_ROOT = 	"http://www.worldofspectrum.org/"
GAMES_URL = SITE_ROOT + "games/" 
STARTING_PAGE = GAMES_URL + "index.html"
FILES_PATH = './files/'
ADDITIONAL_PATH = './additional/'

sessions = [requests.Session() for _ in range(3)]

headers = [{"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Encoding": "gzip, deflate", 
           "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
           "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:49.0) Gecko/20100101 Firefox/49.0"},
           {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Encoding": "gzip, deflate", 
           "Accept-Language": "en-US,ru;q=0.8,en-GB;q=0.5,en;q=0.3",
           "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36"},
           {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Encoding": "gzip, deflate", 
           "Accept-Language": "en-US,ru;q=0.8,zh-CN;q=0.5,en;q=0.3",
           "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}]
         
conn = psycopg2.connect("dbname='wos' user='postgres' host='192.168.1.111' password='123'")
cur = conn.cursor()

try:
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name='games_index'")
    if not bool(cur.rowcount):
        scrap_games_index()
except Exception as e:
    print(e)

try:
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name='games_pages'")
    if not bool(cur.rowcount):
        scrap_games_pages()
    else:
        cur.execute("SELECT * FROM games_index WHERE complete = FALSE")
        if bool(cur.rowcount):
            scrap_games_page()

except Exception as e:
    print(e)

try:
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name='games'")
    if not bool(cur.rowcount):
        scrap_games()
    else:
        cur.execute("SELECT * FROM games_pages WHERE complete = FALSE")
        if bool(cur.rowcount):
            scrap_games()

except Exception as e:
    print(e)

try:
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name='hardware_pages'")
    if not bool(cur.rowcount):
        scrap_hardware_pages()
except Exception as e:
    print(e)

try:
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name='hardware'")
    if not bool(cur.rowcount):
        scrap_hardware()
    else:
        cur.execute("SELECT * FROM hardware_pages WHERE complete = FALSE")
        if bool(cur.rowcount):
            scrap_hardware()

except Exception as e:
    print(e)

try:
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name='games_files'")
    if not bool(cur.rowcount):
        scrap_game_files()
    else:
        cur.execute("SELECT * FROM games_pages WHERE files_complete = FALSE")
        if bool(cur.rowcount):
            scrap_game_files()

except Exception as e:
    print(e)

#conn.commit()
cur.close()
conn.close()
