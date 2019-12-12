#!/usr/bin/python 
import sys, cx_Oracle, argparse, os 

def createprc(db):
     ca = db.cursor() 
     ca.execute("""create or replace function fetch_schema_metadata(inowner        in varchar2, 
                                                                          intype         in varchar2,
                                                                          inname         in varchar2)
  return clob authid current_user is
  dmh     number;
  ddltext clob;
 /* type ott is varray(9) of varchar2(32);
  ots ott := ott('table',
                 'view',
                 'package',
                 'type',
                 'sequence_disabled',
                 'materialized view',
                 'index',
                 'procedure',
                 'trigger');*/
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
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'REMAP_SCHEMA', inowner , NULL);
  /*
  for ot in ots.first .. ots.last loop
    for t in (select object_name
                from all_objects
               where lower(owner) = lower(in_owner)
                 and lower(object_name) like lower(in_name_filter)
                 and lower(object_type) = lower(ots(ot))
               order by object_name) loop
               */
      begin
        dbms_lob.append(ddltext, dbms_metadata.get_ddl(object_type => replace(intype,
                                                                     ' ',
                                                                     '_'),
                                              name        => upper(inname),
                                              schema      => upper(inowner)));
      exception
        when others then
          dbms_lob.append(ddltext, sqlerrm);
      end;
      /*
    end loop;
  end loop;*/
  dbms_metadata.close(dmh);
  return ddltext;
end;
""")

def createprc2(db):
     ca = db.cursor() 
     ca.execute("""
create or replace function gen_insert(p_tab_name VARCHAR2) return clob IS
  v_column_list     VARCHAR2(4096) := null;
  v_insert_list     VARCHAR2(16096);
  v_ref_cur_columns VARCHAR2(16096) := null;
  v_ref_cur_query   VARCHAR2(16000);
  v_ref_cur_output  VARCHAR2(16000) := null;
  v_column_name     VARCHAR2(256);
  ddltext           clob;
  CURSOR c1 IS
    SELECT column_name, data_type
      FROM user_tab_columns
     WHERE table_name = p_tab_name
     ORDER BY column_id;
  refcur sys_refcursor;
BEGIN
  --DBMS_OUTPUT.ENABLE(NULL); -- this will hangle dbms_output for huge table.
  dbms_lob.createtemporary(ddltext, true);
  begin
    FOR i IN c1 LOOP
      v_column_list := v_column_list || ',' || i.column_name;
      IF i.data_type = 'NUMBER' THEN
        v_column_name := i.column_name;
      ELSIF i.data_type = 'DATE' THEN
        v_column_name := chr(39) || 'to_date(' || chr(39) || '||chr(39)' ||
                         '||to_char(' || i.column_name || ',' || chr(39) ||
                         'dd/mm/yyyy hh24:mi:ss' || chr(39) ||
                         ')||chr(39)||' || chr(39) || ', ' || chr(39) ||
                         '||chr(39)||' || chr(39) ||
                         'dd/mm/rrrr hh24:mi:ss' || chr(39) ||
                         '||chr(39)||' || chr(39) || ')' || chr(39);
      ELSIF i.data_type = 'VARCHAR2' then
        -- Following line will hangle single quote in text data.
        v_column_name := 'chr(39)||replace(' || i.column_name ||
                         ','''''''','''''''''''')||chr(39)';
      
      ELSIF i.data_type = 'CHAR' then
        v_column_name := 'chr(39)||' || i.column_name || '||chr(39)';
      END IF;
      v_ref_cur_columns := v_ref_cur_columns || '||' || chr(39) || ',' ||
                           chr(39) || '||' || v_column_name;
    END LOOP;
  
    IF v_column_list is null then
      --dbms_output.put_line('--Table '|| p_tab_name || ' does not exist');
      dbms_lob.append(ddltext,
                      '--Table ' || p_tab_name || ' does not exist');
    ELSE
    
      v_column_list     := LTRIM(v_column_list, ',');
      v_ref_cur_columns := SUBSTR(v_ref_cur_columns, 8);
    
      v_insert_list   := 'INSERT INTO ' || p_tab_name || ' (' ||
                         v_column_list || ') VALUES ';
      v_ref_cur_query := 'SELECT ' || v_ref_cur_columns || ' FROM ' ||
                         p_tab_name;
    
      OPEN refcur FOR v_ref_cur_query;
      LOOP
        FETCH refcur
          INTO v_ref_cur_output;
        EXIT WHEN refcur%NOTFOUND;
        v_ref_cur_output := '(' || v_ref_cur_output || ');';
        v_ref_cur_output := REPLACE(v_ref_cur_output, ',,', ',null,');
        v_ref_cur_output := REPLACE(v_ref_cur_output, ',,', ',null,');
        v_ref_cur_output := REPLACE(v_ref_cur_output, '(,', '(null,');
        v_ref_cur_output := REPLACE(v_ref_cur_output, ',,)', ',null)');
        v_ref_cur_output := REPLACE(v_ref_cur_output,
                                    'null,)',
                                    'null,null)');
      
        v_ref_cur_output := v_insert_list || v_ref_cur_output;
        --DBMS_OUTPUT.PUT_LINE (v_ref_cur_output);
        --dbms_lob.append(ddltext, chr(13) || chr(10) || v_ref_cur_output);
        dbms_lob.append(ddltext, chr(13) || v_ref_cur_output);
      END LOOP;
      IF v_ref_cur_output is null then
        --dbms_output.put_line('--No data in '||p_tab_name);
        dbms_lob.append(ddltext, '--No data in ' || p_tab_name);
      END IF;
    END IF;
  
  Exception
    When others then
      --dbms_output.put_line('Error='||sqlerrm);
      dbms_lob.append(ddltext, 'Error=' || sqlerrm);
  end;
  return ddltext;
END;
""")

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-s', action='store', dest='schema',  help='Schema name')
parser.add_argument('-p', action='store', dest='password',  help='Schema password')
parser.add_argument('-t', action='store', dest='tns',  help='TNS name')
parser.add_argument('-o', action='store', dest='owner',  help='owner name')
parser.add_argument('-path', action='store', dest='path',  help='source path ')
parser.add_argument('-tables', action='store', dest='tables',  help='tables for exporting data ')


args = parser.parse_args()


db = cx_Oracle.connect(args.schema,args.password,args.tns)
createprc(db)
createprc2(db)

ca = db.cursor() 
sch = ca.execute(ca.prepare("select distinct object_name, object_type from all_objects where lower(owner) = lower('" + args.owner  + "')  and lower(object_name) like lower('%')")).fetchall() 

for aline  in [x for x in sch]: 
     q = ca.prepare('select fetch_schema_metadata(:1, :2,:3) as text from dual') 
     c = db.cursor() 
     print aline[0].upper()
     res = ca.execute(q, [ args.owner, aline[1].upper(), aline[0].upper(),]) 
     dirpath  = args.path + "\\" + aline[1] 
     if not os.path.exists( dirpath ):
         os.makedirs(dirpath )

     f = open( dirpath + '\\%s.sql' % (aline[0]), 'w') 
     f.write(res.fetchall()[0][0].read())
     f.close()
     c.close()

if  args.tables :
    dirpath  = args.path + "\\" + 'data' 
    if not os.path.exists( dirpath ):
        os.makedirs(dirpath )

    for tbls in  args.tables.split(',') :
        q = ca.prepare('select gen_insert(:1) as text from dual') 
        c = db.cursor() 
        res = ca.execute(q, [ tbls,]) 
        f = open( dirpath + '\\%s.sql' % (tbls), 'w') 
        f.write(res.fetchall()[0][0].read())
        f.close()
        c.close()