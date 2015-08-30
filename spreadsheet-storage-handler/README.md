# Spreadsheet Storage Handler

This storage handler allow a user to place spreadsheets (.xlsx format) in their Hdfs home directory and then perform SQL queries against them via Presto.

After downloading the tarball (link here) extract into the plugin directory in your Presto installation.

Next configure your spreadsheet catalog by adding a `spreadsheet.properties` file in the `etc/catalog` directory in your Presto installation.

#### Example spreadsheet.properties file:

    connector.name=spreadsheet
    basepath=hdfs://<namenode>/user
    subdir=spreadsheets

#### Usage

Next place your desired spreadsheet in your home directory.  For example if your username is `user1` then you would need to place the spreadsheet in the `hdfs://<namenode>/user/user1/spreadsheets` directory.  Once the file is in place a new schema in the spreadsheet catalog will appear.  Each sheet in the spreadsheet will be represented as table in Presto.

#### Types

Spreadsheets are very flexible allowing multiple types per column, however this flexiblity does not easily translate to a relational table.  This plugin handles this impedance mismatch by examining each cell in the column.  If there only one type (String, Boolean, Double) is detected the column is labeled the same name as the spreadsheet column (A, B, C, etc).  However if multiple types are detected then a column for each type is created (A_NUMBER, A_STRING, etc) and the cells are null for the values that are a different type.  For example:

##### Spreadsheet
| |A|B|C|D|
|---|---|:---:|---|---|
|1|abc|3|TRUE|d|
|2|123|4|FALSE|   |
|3|true|5|TRUE|g|

##### Presto Table
| |a_boolean|a_number|a_string|b|c|d|
|---|---|---|---|:---:|---|---|
|1|NULL|NULL|abc|3|TRUE|d|
|2|NULL|123|NULL|4|FALSE|NULL|
|3|true|NULL|NULL|5|TRUE|g|