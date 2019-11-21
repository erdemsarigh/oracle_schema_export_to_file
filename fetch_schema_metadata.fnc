create or replace function fetch_schema_metadata(in_owner       in varchar2,
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
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',                                    FALSE);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',                                    FALSE);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',                                    true);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',                                    true);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'FORCE',                                    false);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS_AS_ALTER',                                    true);
  dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',                                    FALSE);
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
/
