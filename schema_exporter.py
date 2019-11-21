#!/usr/bin/python 
import sys, cx_Oracle 

db = cx_Oracle.connect('schema name', 'password', 'tnsname')
ca = db.cursor() 
sch = ca.execute(ca.prepare("select username from all_users where username in ('schema name', 'OTHER_SCHEMA') order by username")).fetchall() 
q = ca.prepare('select fetch_schema_metadata(:1, \'%\') as text from dual') 
for schema in [x[0] for x in sch]: 
     c = db.cursor() 
     res = ca.execute(q, [schema.upper(), ]) 
     f = open('%s.sql' % (schema), 'w') 
     f.write(res.fetchall()[0][0].read())
     f.close()
     c.close()
 