# tableau_redshift_to_snowflake
Convert Tableau Workbooks from Redshift to Snowflake

**Fair Warning: Tableau does not recommend directly modifying the XML. Please ensure you have a backup copy of the packaged workbook for testing / validation prior to proceeding with the conversion.**

This script can be used to modify Tableau's XML to repoint all current Redshift connections to Snowflake as can be used as an alternative to Tableau's replace data source option. This script will only modify Redshift connections and leave any other classes of connections in their original form.

This script only uses base Python packages (tested with Python 3.7 and above)

It can only be run against Tableau packaged workbooks (.twbx) but will create an unpackaged version within a subfolder as well as packaged with "-Snowflake" postfix as part of conversion.

The below items need to be set within the script prior to running<br />
``` 
ACCOUNT_NAME = ""
USER_NAME = ""
DB_NAME = ""
WAREHOUSE = ""
SCHEMA = ""
ROLE_NAME = ""
```

One the above are set the script can be run via the following command <br /> `python tableau_redshift_to_snowflake_migrator.py MyWorkbook.tbwx`

Thank you to https://gist.github.com/gnilrets/bc1e85aee26105013b08e038b19a42f7 and https://github.com/calogica/tableau-redshift-snowflake-converter/blob/master/Tableau%20Redshift%20to%20Snowflake%20Migration%20Script.ipynb for setting the groundwork.
