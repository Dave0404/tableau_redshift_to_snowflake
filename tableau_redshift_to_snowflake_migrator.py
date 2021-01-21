'''
Tableau Workbook Redshift to Snowflake Migrator
=============================================
This is heavily based on previous work by the below individuals
https://github.com/calogica/tableau-redshift-snowflake-converter/blob/master/Tableau%20Redshift%20to%20Snowflake%20Migration%20Script.ipynb
https://gist.github.com/gnilrets/bc1e85aee26105013b08e038b19a42f7

Fair Warning: Modify the XML is not recommended by Tableau. Please ensure you've got a backup copy of the workbook for comparison / validation purposes.

This script converts Tableau packaged workbooks from Redshift to Snowflake.  It works by
parsing the workbook XML.  It will replace datasource connections so they point to your
Snowflake instance instead of Redshift.  Additionally, the script will UPCASE schemas,
tables, and any columns that don't have characters that need to be quoted. Lastly the code
inserts an InitialSQL property into the connection so that the correct schema is set to preserve
custom SQL.

Known limitations:
  - This will not work on converting published data sources.

Usage:
  Ensure the following variables are set prior to running:
    ACCOUNT_NAME = ""
    USER_NAME = ""
    DB_NAME = ""
    WAREHOUSE = ""
    SCHEMA = ""
    ROLE_NAME = ""
  Run the script and specify the path to the Tableau workbook, relative to this script:
    >>> python tableau_redshift_to_snowflake_migrator.py MyWorkbook.tbwx
'''

import sys
import io
import os
import zipfile
import shutil
import re
import logging
import xml.etree.ElementTree

#Set the below variables prior to running against any existing Tableau workbooks.
ACCOUNT_NAME = ""
USER_NAME = ""
DB_NAME = ""
WAREHOUSE = ""
SCHEMA = ""
ROLE_NAME = ""

logging.basicConfig()
LOG = logging.getLogger('migrator')
LOG.setLevel(logging.INFO)


def migrate_to_snowflake(packaged_workbook_path):
    unpacked_workbook = _unpack(packaged_workbook_path)
    workbook_xml = _migrate_xml(unpacked_workbook['workbook_path'])
    _save_migrated_workbook(workbook_xml, unpacked_workbook['workbook_path'])
    _repack(unpacked_workbook['extract_dir'], file_ext=unpacked_workbook['file_ext'])


def _unpack(packaged_workbook_path):
    file_ext = f'.{packaged_workbook_path.split(".")[-1]}'
    packaged_workbook_path = os.path.abspath(packaged_workbook_path)
    extract_dir = packaged_workbook_path.replace(file_ext, '')

    if not os.path.exists(packaged_workbook_path):
        raise Exception(f'File {packaged_workbook_path} does not exist')

    with zipfile.ZipFile(packaged_workbook_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    LOG.info('Unzipped %s to %s', packaged_workbook_path, extract_dir)

    file_ext_wb = file_ext.strip('x')

    file_path_wb = packaged_workbook_path.replace(file_ext, file_ext_wb)
    file_path_wb = os.path.join(extract_dir, os.path.basename(file_path_wb))

    return {'extract_dir': extract_dir, 'workbook_path': file_path_wb, 'file_ext': file_ext}


def _repack(extract_dir, file_ext='.twbx'):
    zip_filename = f'{extract_dir}-Snowflake{file_ext}'
    LOG.info('Repackaging as %s', zip_filename)

    zipf = zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, extract_dir)
            zipf.write(full_path, arcname=rel_path)


def _migrate_xml(workbook_path):
    tree, replace_vars = _xml_replacements(workbook_path)
    doc = _global_replacements(tree, replace_vars)
    return doc


def _xml_replacements(workbook_path):
    tree = xml.etree.ElementTree.parse(workbook_path)
    root = tree.getroot()
    replace_vars = {}

    for datasource in root.findall('.//datasource'):
        if not (datasource.get('hasconnection') or datasource.get('hasconnection') is None):
            continue

        for named_connection in datasource.findall('.//named-connection'):
            LOG.debug(
                'Replacing %s %s %s',
                named_connection.tag,
                named_connection.get('name'),
                named_connection.get('caption'),
            )

            named_connection.set('caption', f'{ACCOUNT_NAME}.snowflakecomputing.com')
            named_connection.set(
                'name', named_connection.get('name').replace('redshift', 'snowflake')
            )

            LOG.debug(
                '>> with %s %s %s',
                named_connection.tag,
                named_connection.get('name'),
                named_connection.get('caption'),
            )

            for connection in list(named_connection):
                if connection.get('class')=="redshift":
                    connection.set('class', 'snowflake')
                    connection.set('schema', f'{SCHEMA}')
                    connection.set('dbname', f'{DB_NAME}')
                    connection.set('server', f'{ACCOUNT_NAME}.snowflakecomputing.com')
                    connection.set('service', f'{ROLE_NAME}')
                    connection.set('username', f'{USER_NAME}')
                    connection.set('warehouse', f'{WAREHOUSE}')
                    connection.set('one-time-sql', f'USE SCHEMA {SCHEMA};')
                    connection.set('port', '')
                else:
                    continue

        for relation in datasource.iter('relation'):
            if relation.get('connection') is None:
                continue

            relation.set('connection', relation.get('connection').replace('redshift', 'snowflake'))
            if relation.get('table') is None:
               continue
            relation.set('table', relation.get('table').upper())
            relation.set('table', relation.get('table').replace('PUBLIC', f'{schema}'))

            redshift_name = relation.get('name')
            snowflake_name = redshift_name.upper()
            replace_vars[redshift_name] = snowflake_name
            relation.set('name', snowflake_name)

        for metadata_record in datasource.findall('.//metadata-record'):
            if metadata_record.get('class') != 'column':
                continue
            for metadata in list(metadata_record):
                if metadata.tag in ['remote-name', 'remote-alias']:
                    snowflake_value = metadata.text
                    has_quotables = re.search(r'[^a-z0-9_]', metadata.text) is not None
                    starts_with_num = re.search(r'^[0-9]', metadata.text) is not None
                    if not (has_quotables or starts_with_num):
                        snowflake_value = metadata.text.upper()
                        replace_vars[metadata.text] = snowflake_value

                    LOG.debug(
                        'Replacing %s %s with %s', metadata.tag, metadata.text, snowflake_value
                    )
                    metadata.text = snowflake_value

                if metadata.tag == 'parent-name':
                    metadata.text = metadata.text.upper()

    return (tree, replace_vars)


def _global_replacements(tree, replace_vars):
    with io.BytesIO() as bs:
        tree.write(bs)
        doc = bs.getvalue().decode()

    for redshift_value, snowflake_value in replace_vars.items():
        doc = doc.replace(f'[{redshift_value}]', f'[{snowflake_value}]')
    return doc


def _save_migrated_workbook(doc, file_path_wb):
    with open(file_path_wb, 'w') as xmlfile:
        xmlfile.write(doc)


if __name__ == '__main__':
    migrate_to_snowflake(sys.argv[1])
