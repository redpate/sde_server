sde_server
=====

Working with SDE from EVE Online

Build
-----

    $ rebar3 compile
    
Usage
-----
Start application
    application:start(sde_server).
Use `sde_dir` and `priv_dir` env parametrs to config directories

Parse yaml file to dets table
``` erl
    %% simple way
    sde_server:parse_yaml(YamlFile).
    %% define type of tables, ram_file etc. Options should be list
    parse_yaml(FileName, TableOptions). 
    %% define parse function 
    parse_yaml(FileName, ParseFunction).
    %% or both
    parse_yaml(FileName, TableOptions, ParseFunction).
```
Get dets reference
``` erl
    sde_server:get_table(TableName,Index).
```

Delete table

``` erl
    sde_server:delete_dets({TableName,Index}).
```
# TODO
- ets clients
- read/write concurrency
- generator callback
- specs/documentation