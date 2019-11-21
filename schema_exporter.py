#!/usr/bin/python 
import sys, cx_Oracle, argparse


# parser = argparse.ArgumentParser(description='Export Oracle object as text.')
# parser.add_argument('schema', dest='schema', help='schema name')
# parser.add_argument('password',dest='password', help='schema password')
# parser.add_argument('TNS',dest='TNS', help='TNS name')

# args = parser.parse_args()
# print args.schema

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-s', action='store', dest='schema',  help='Schema name')
parser.add_argument('-p', action='store', dest='password',  help='Schema password')
parser.add_argument('-t', action='store', dest='tns',  help='TNS name')

args = parser.parse_args()


db = cx_Oracle.connect(args.schema,args.password,args.tns)

ca = db.cursor() 
sch = ca.execute(ca.prepare("select object_name from all_objects where lower(owner) = lower('" + args.schema  + "')  and lower(object_name) like lower('%')")).fetchall() 
for objectname in [x[0] for x in sch]: 
     q = ca.prepare('select fetch_schema_metadata(:1, :2) as text from dual') 
     c = db.cursor() 
     res = ca.execute(q, [ args.schema,  objectname.upper(),]) 
     f = open('%s.sql' % (objectname), 'w') 
     f.write(res.fetchall()[0][0].read())
     f.close()
     c.close()
 