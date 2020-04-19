# DBMerge

#### Description
This tool is for diff tables in Oracle database.  
When you rewrite system or upgrade system, it often used by diff new and old batch results 

#### Overview
    ・DBMerge-run - The demo of this DBMerge.You can copy it anywhere to try.  
    ・DBMerge-src - The source of this DBMerge.  
    ・DBMerge-testData - Test data and test ddl for the demo.  

#### Demo
    ・DBMerge-run/config/jdbc.properties  
    ・DBMerge-run/config/merge.properties  
    ・DBMerge-run/output/  
    ・DBMerge-run/lib/ojdbc6-11.2.0.3.jar  
    ・DBMerge-run/tables/  
    ・AutoCreate.jar  
    ・DBMerge.jar  

#### DBMerge-run/config/jdbc.properties  
    The diff database config file.  
    Only support of Oracle database.  

    # the new db info
    new.db.driver=oracle.jdbc.driver.OracleDriver
    new.db.url=jdbc:oracle:thin:@192.168.99.100:1521:xe
    new.db.username=DBM_NEW
    new.db.password=password

    # the new db info
    old.db.driver=oracle.jdbc.driver.OracleDriver
    old.db.url=jdbc:oracle:thin:@192.168.99.100:1521:xe
    old.db.username=DBM_OLD
    old.db.password=password

    # only need for create properties of tables
    new.create.schema.name=DBM_NEW
    old.create.schema.name=DBM_OLD
    comm.create.table.name=TABLE%

#### DBMerge-run/config/merge.properties
    The merge file names for diff in [DBMerge-run/tables/].
    You can control this file to diff the table what you want, the merge file would be create by only once.

    TABLE1.properties
    TABLE2.properties
    TABLE3.properties

#### DBMerge-run/output/
    The Merge result output folder.
    One table merge result would be output by three files like this
    TABLE1.properties_diffColumn.txt
    TABLE1.properties_newOnly.txt
    TABLE1.properties_oldOnly.txt

#### DBMerge-run/lib/ojdbc6-11.2.0.3.jar
    The JDBC driver of oracle.

#### DBMerge-run/tables/
    The merge config of tables.
    You can use [AutoCreate.jar] to auto create merge config.

    # merge info of new table 
    new.table.name=TABLE1
    new.table.key.column=COLUMN1
    new.table.merge.column=COLUMN1,COLUMN2,COLUMN3,COLUMN4,COLUMN5,COLUMN6,COLUMN7,COLUMN8,COLUMN9,COLUMN10,COLUMN11
    new.table.merge.filter=

    # merge info of old table 
    old.table.name=TABLE1
    old.table.key.column=COLUMN1
    old.table.merge.column=COLUMN1,COLUMN2,COLUMN3,COLUMN4,COLUMN5,COLUMN6,COLUMN7,COLUMN8,COLUMN9,COLUMN10,COLUMN11
    old.table.merge.filter=


#### VS. 

#### Requirement
    ・JAVA1.5 or earlier
    ・CPU 2.4GHZ at least
    ・MOMERY 4G at least

#### Usage
    You can double click on it for run or use the command  
    java -jar DBMerge.jar  

    When you use the command, you can set the mutial thread parameters like this  
    java -jar DBMerge.jar  10 10  

#### Install
    You need only copy the [DBMerge-run]  on your PC or server

#### Contribution
    1. Fork it ( https://github.com/vekee/DBMerge.git )
    2. Create your feature branch (git checkout -b my-new-feature)
    3. Commit your changes (git commit -am 'Add some feature')
    4. Push to the branch (git push origin my-new-feature)
    5. Create new Pull Request

#### Licence
[MIT](https://github.com/vekee/DBMerge/blob/dev/LICENSE.md)

#### Author
https://apasys.co.jp/  
DUAN DAHAI  