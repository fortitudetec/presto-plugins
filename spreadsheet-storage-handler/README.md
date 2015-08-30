# Spreadsheet Storage Handler

This storage handler allow a user to place spreadsheets (.xlsx format) in their Hdfs home directory and then perform SQL queries against them via Presto.

After downloading the tarball (link here) extract into the plugin directory in your Presto installation.

Next configure your spreadsheet catalog by adding a spreadsheet.properties file in the etc/catalog directory in your Presto installation.

#### Example spreadsheet.properties file:

    connector.name=spreadsheet
    basepath=hdfs://<namenode>/user
    subdir=spreadsheets

#### Usage

Next place your desired spreadsheet in your home directory.  For example if your username is `user1` then you would need to place the spreadsheet in the `hdfs://<namenode>/user/user1/spreadsheets` directory.  Once the file is in place a new schema in the spreadsheet catalog will appear.  Each sheet in the spreadsheet will be represented as table in Presto.