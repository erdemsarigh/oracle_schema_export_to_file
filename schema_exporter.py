#!/usr/bin/python 
import sys, cx_Oracle, argparse, os 


# parser = argparse.ArgumentParser(description='Export Oracle object as text.')
# parser.add_argument('schema', dest='schema', help='schema name')
# parser.add_argument('password',dest='password', help='schema password')
# parser.add_argument('TNS',dest='TNS', help='TNS name')

# args = parser.parse_args()
# print args.schema

def createprc(db):
     ca = db.cursor() 
     sch = ca.execute("""create or replace function fetch_schema_metadata(in_owner       in varchar2,
                                                 in_name_filter in varchar2 default '%')
  return clob authid current_user is
  dmh     number;
  ddltext clob;
  type ott is varray(9) of varchar2(32);
  ots ott := ott('table',
                 'view',
                 'package',
                 'type',
                 'sequence_disabled',
                 'materialized view',
                 'index',
                 'procedure',
                 'trigger');
begin
  dbms_lob.createtemporary(ddltext, true);
  dmh := dbms_metadata.open('TABLE');
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',FALSE);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',FALSE);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'FORCE',false);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS_AS_ALTER',true);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',FALSE);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'REMAP_SCHEMA', in_owner , NULL);
  
  for ot in ots.first .. ots.last loop
    for t in (select object_name
                from all_objects
               where lower(owner) = lower(in_owner)
                 and lower(object_name) like lower(in_name_filter)
                 and lower(object_type) = lower(ots(ot))
               order by object_name) loop
      begin
        dbms_lob.append(ddltext, dbms_metadata.get_ddl(object_type => replace(upper(ots(ot)),
                                                                     ' ',
                                                                     '_'),
                                              name        => upper(t.object_name),
                                              schema      => upper(in_owner)));
      exception
        when others then
          dbms_lob.append(ddltext, sqlerrm);
      end;
    end loop;
  end loop;
  dbms_metadata.close(dmh);
  return ddltext;
end;
""")


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-s', action='store', dest='schema',  help='Schema name')
parser.add_argument('-p', action='store', dest='password',  help='Schema password')
parser.add_argument('-t', action='store', dest='tns',  help='TNS name')
parser.add_argument('-path', action='store', dest='path',  help='source path ')


args = parser.parse_args()


db = cx_Oracle.connect(args.schema,args.password,args.tns)
createprc(db)

ca = db.cursor() 
sch = ca.execute(ca.prepare("select object_name, object_type from all_objects where lower(owner) = lower('" + args.schema  + "')  and lower(object_name) like lower('%')")).fetchall() 

for aline  in [x for x in sch]: 
     q = ca.prepare('select fetch_schema_metadata(:1, :2) as text from dual') 
     c = db.cursor() 
     res = ca.execute(q, [ args.schema,  aline[0].upper(),]) 
     dirpath  = args.path + "\\" + aline[1] 
     if not os.path.exists( dirpath ):
         os.makedirs(dirpath )

     f = open( dirpath + '\\%s.sql' % (aline[0]), 'w') 
     f.write(res.fetchall()[0][0].read())
     f.close()
     c.close()
 