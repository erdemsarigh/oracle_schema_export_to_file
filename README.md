# Oracle Schema Export to File

## Under The Hood

This script creates functions named `fetch_schema_metadata` and `gen_insert` in the Oracle Instance. The function `fetch_schema_metadata` fed through `all_objects` view and the result saved to disk as a single sql file per object.

## Prerequisites

- Python 3
- cx_Oracle module (to install: `pip install cx_Oracle` )
- Oracle Data Access Components (ODAC) xcopy version

## Usage

```python
py schema_exporter.py [-s SCHEME_NAME] [-p PASSWORD] [-t TNS_NAME] [-o OBJECT_OWNER] [-path PATH_TO_EXPORT] [-tables TABLES]
```

| Argument      | Description   |
| ------------- |-----------------------------------------------|
| -s            | Denotes oracle scheme for exporting objects   |
| -p            | Oracle user password in scheme                |
| -t            | TNSNAME of the Instance                       |
| -o            | Owner name of objects                         |
| -path         | Folder path to export                         |
| -tables       | Comma seperated table list for exporting      |
