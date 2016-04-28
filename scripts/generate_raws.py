#!/usr/bin/env python
import MySQLdb
import os

# Target files
home = os.path.expanduser("~")
course_list = home + "/Code/irt/data/courselist.txt"
db_auth = home + "/.ssh/mysql_user"
exports_dir = home + "/Code/irt/sql/"

# Set up database auth
with open(db_auth, 'r') as f:
    dbuser = f.readline().rstrip()
    dbpass = f.readline().rstrip()

# Read in export query templates
queries = []
for query_file in os.listdir(exports_dir):
    with open(os.getcwd() + "/sql/" + query_file, 'r') as query:
        queries.append(query.read())

# Read in course list
courses = []
with open(course_list, 'r') as clist:
    for line in clist.readlines():
        courses.append(line.rstrip())

# Connect to database
db = MySQLdb.connect(host='127.0.0.1', port=3306, user=dbuser, passwd=dbpass)
cursor = db.cursor()

# Run loaded queries over each course
for course in courses:
    for query in queries:
        outdir = course.replace('/', '_')
        cursor.execute(query.format(outdir, course))

# Close database
cursor.close()
db.close()
