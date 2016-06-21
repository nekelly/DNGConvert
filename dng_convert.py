from __future__ import print_function
import os
import time
from datetime import datetime
import shutil
import subprocess
import sqlite3
from Queue import Queue
from threading import Thread, Lock

###########
# Constants
###########

d_source_vol = {'LUMIX':'RW2', 'EOS':'CR2'}
source_dir = '\\DCIM'
target_drive = 'C:'
target_dir = '\\Noel\\Photos'
target_path = target_drive + target_dir
dng_file_ext = "dng"
db_path = 'C:\Noel\Photos\import.db'
dng_converter = 'C:\\Program Files (x86)\\Adobe\\Adobe DNG Converter.exe'

# Classes

class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try: func(*args, **kargs)
            except Exception, e: print(e)
            self.tasks.task_done()

class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads): Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()

###########
# Functions
###########

def bail(exit_code):
    con.close()
    exit(exit_code)


def get_drive_list():
    # Find all accessible drive letters
    l_drives=[]
    for letter in ['D', 'E', 'F', 'G', 'H', 'I', 'J']:
        if os.path.isdir(letter + ':'):
            l_drives.append(letter + ':')

    return l_drives


def find_source_drive():
    source_drive = ''
    file_type = ''    # File extension

    for drive in get_drive_list():
        print("Checking %s ..." % drive)
        vol_cmd = "vol " + drive
        p = subprocess.Popen(vol_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()

        for vol_name in d_source_vol.keys():
            if vol_name in stdout:
                return drive, d_source_vol[vol_name]


def get_convert_lists(source_file_list):

    source_list = []
    target_list = []

    for source_file_path in source_file_list:
        source_file_name = source_file_path.split("\\")[-1]

        create_date = time.strftime("%Y-%m-%d", time.gmtime(os.stat(source_file_path)[9]))
        dng_file_name = source_file_path.replace(file_type, dng_file_ext)
        #temp_raw_file_path = "%s\\%s\\%s" % (target_path, create_date, source_file)
        target_dng_file_path = "%s\\%s\\%s" % (target_path, create_date, dng_file_name)
        #date_path = target_path + "\\" + create_date

        row = (target_dng_file_path,)
        global cur
        cur.execute("select count(*) from imported_files where dng_file = ?", row)
        row_count = cur.fetchone()[0]

        if row_count == 0:
            source_list.append(source_file_path)
            target_list.append(target_dng_file_path)
        else:
            print("Skipping '%s'." % source_file_path)

    return (source_list, target_list)


def get_target_convert_list(source_file_list):

    target_list = []

    for source_file_path in source_file_list:
        source_file_name = source_file_path.split("\\")[-1]

        create_date = time.strftime("%Y-%m-%d", time.gmtime(os.stat(source_file_path)[9]))
        dng_file_name = source_file_path.replace(file_type, dng_file_ext)
        #temp_raw_file_path = "%s\\%s\\%s" % (target_path, create_date, source_file)
        target_dng_file_path = "%s\\%s\\%s" % (target_path, create_date, dng_file_name)
        #date_path = target_path + "\\" + create_date


def get_file_dates(source_file_list):

    date_list = []
    for file_path in source_file_list:
        #file_name = file_path.split("\\")[-1]
        create_date = time.strftime("%Y-%m-%d", time.gmtime(os.stat(file_path)[9]))
        if create_date not in date_list:
            date_list.append(create_date)

    return date_list


def convert_raw_file(source_file_path):

    source_file = source_file_path.split("\\")[-1]
    create_date = time.strftime("%Y-%m-%d", time.gmtime(os.stat(source_file_path)[9]))
    dng_file_name = source_file.replace(file_type, dng_file_ext)
    temp_raw_file_path = "%s\\%s\\%s" % (target_path, create_date, source_file)
    target_dng_file_path = "%s\\%s\\%s" % (target_path, create_date, dng_file_name)

    threadLock.acquire()
    print("Converting '%s' --> '%s'" % (source_file_path, target_dng_file_path))
    threadLock.release()

    shutil.copyfile(source_file_path, temp_raw_file_path)
    convert_cmd = '"%s" -c %s' % (dng_converter, temp_raw_file_path)

    rc = subprocess.call(convert_cmd, shell=True)
    if rc != 0:
        print("Conversion of '%s' Failed!" % temp_raw_file_path)
        return False

    file_delete_list.append(temp_raw_file_path)

    return True


def create_date_dirs(date_list):

    for date in date_list:
        date_path = target_path + "\\" + date

        if not os.path.isdir(date_path):
            print("Creating directory '%s' ... " % date_path, end="")
            try:
                os.mkdir(date_path)
                print("OK")
            except OSError:
                print("ERROR: failed to create directory '%s'." % date_path)
                return False

    return True


def log_files_to_db(file_list):

    for file in file_list:
        try:
            cur.execute("insert into imported_files values(?)", (file,))
        except:
            print("Error inserting row in to 'imported_files'.")
            return False

    try:
        con.commit()
    except:
        print("Error committing rows inserted into 'imported_files'.")
        return False

    return True

def reimport(date_list):

    for date in date_list:
        print("Deleting rows for date '%s'." % date)
        cur.execute("delete from imported_files where dng_file like 'C:\\Noel\\Photos\\2016\\%s%%'" % date)

    con.commit()
    con.close()


#######
# Main
#######

# Print header
print()
print("=" * 80)
print("#                                                                              #")
print("#                     RAW File Conversion Utility v1.0                         #")
print("#                                                                              #")
print("=" * 80)
print()

# Check if the DNG converter executable exists
if not os.path.exists(dng_converter):
    print("Error, DNG converter not found.")
    exit(1)

source_drive, file_type =  find_source_drive()

# Get the year
year = datetime.now().strftime("%Y")
target_path = target_path + "\\" + year

if source_drive == '':
    print("Drive not found. Press <enter> to exit.")
    raw_input()
    exit(1)
else:
    source_path = source_drive + source_dir
    print("Source location: %s" % source_path)
    print("Target location: %s" % target_path)

print()

"""
This database is to record which files have already been imported.
Useful when the converted files have been deliberately deleted
and you don't want them imported again.
"""

con = sqlite3.connect(db_path, check_same_thread = False)
cur = con.cursor()
cur.execute("CREATE TABLE if not exists imported_files (dng_file)")
con.commit()

#reimport(['2016-06-18'])
raw_input("Press <enter>")
#exit()

threadLock = Lock()

file_delete_list = []

# Get a list of all files on the media
l_raw_files = []
l_jpg_files = []

for (path, dirs, files) in os.walk(source_path):
    raw_files = [file for file in files if file.endswith(file_type)]
    jpg_files = [file for file in files if file.endswith('JPG')]

    for raw_file in raw_files:
        source_file_path = "%s\\%s" % (path, raw_file)
        l_raw_files.append(source_file_path)

    for jpg_file in jpg_files:
        source_file_path = "%s\\%s" % (path, jpg_file)
        l_jpg_files.append(source_file_path)		

l_convert_lists = get_convert_lists(l_raw_files)
source_list = l_convert_lists[0]
target_list = l_convert_lists[1]

convert_count = len(source_list)
convert_dates = get_file_dates(source_list)

print()
print("Found %s files on %s" % (len(l_raw_files), source_drive))
print("Converting %d files." % convert_count)
if convert_count == 0:
    bail(0)

print("Converting files for the following dates:")
for date in convert_dates:
    print(date)
print()

try:
    response = raw_input("Continue (Y/N)?: ")
    if response.upper() != "Y":
        exit(1)
except Exception:
    exit(1)

if not create_date_dirs(convert_dates):
    bail(1)

print()

pool = ThreadPool(4)

for source_file in source_list:
    pool.add_task(convert_raw_file, source_file)
pool.wait_completion()

if not log_files_to_db(target_list):
    bail(1)

con.close()

print()
print("Deleting the temporary raw files ... ", end="")

success = True
for file_name in file_delete_list:
    # Delete the temporary copy
    print("Deleting '%s' ... " % file_name, end="")
    try:
        os.remove(file_name)
    except OSError as e:
        error = True
        print("Failed to delete '%s'!" % file_name)
        print(e)

if success:
    print("OK")

print()
raw_input("Press <enter> to exit.")

# The End #