-module(sde_server).
-compile(export_all).  %% remove this

-behaviour(gen_server).
%% API
-export([start_link/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    error_logger:tty(true),
    SdeDir = sde_server_app:get_env(sde_dir, default_sde_dir),
    PrivDir = sde_server_app:get_env(priv_dir, default_priv_dir),
    file:make_dir(SdeDir),  %% create directories if enoent
    file:make_dir(PrivDir),
    gen_server:start_link({local, ?MODULE}, ?MODULE, {SdeDir, PrivDir}, []).
%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init({SdeDir, PrivDir}) ->
    {ok, #{sde_dir => SdeDir,
            pid => self(),
            priv_dir => PrivDir,
            dets_tables => load_all_dets(PrivDir, #{}),
            clients => []
            }
    }.
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({create, dets, _BaseTableName, Options}, _From, #{priv_dir := PrivDir, dets_tables := TablesMap}=State) ->
    _TableName = gen_tablename(_BaseTableName, TablesMap), 
    {TableName, NewTablesMap}=Res = ?MODULE:create_dets(_TableName, PrivDir, Options, TablesMap),
    {reply, Res, State#{dets_tables => NewTablesMap}};
handle_call({create, gen_dets, TableName, Options, GenModule, GenFunction, GenOptiions}, {FromPid, _}, State) ->
    WorkerPid = spawn(?MODULE, gen_dets, [TableName, Options, GenModule, GenFunction, GenOptiions, FromPid, State]),
    {reply, WorkerPid, State}; %% return pid of worker
handle_call({get, dets, TableName, Index}, _From, #{dets_tables := TablesMap}=State) ->
     Res = maps:get(ref,
            maps:get(Index, 
                maps:get(TableName, TablesMap, #{Index => #{ref => {error, invalid_tablename}}}),
            #{ref => {error, invalid_index}}), 
           {error, undefined}),
     {reply, Res, State};
handle_call({get, dets, TableName}, _From, #{dets_tables := TablesMap}=State) ->
    case maps:get(TableName, TablesMap, undefined) of
        undefined->
            {error, invalid_tablename};
        TableNameMap->
            LastIndex = lists:last(lists:usort(maps:keys(TableNameMap))),
            Res = maps:get(ref, maps:get(LastIndex, TableNameMap, #{ref => {error, invalid_index}}), {error, undefined}),
            {reply, Res, State}
    end;
handle_call({delete, dets, TableName, Index}, _From, #{dets_tables := TablesMap}=State) ->
    IsValidTble = is_valid_tablename({TableName, Index}, TablesMap),
    if
        IsValidTble->
            case delete_dets(TableName, Index, TablesMap) of
                {error, Reason}->
                    {reply, {error, Reason}, State};
                NewTablesMap ->
                    {reply, true, State#{dets_tables => NewTablesMap}}
            end;
        true->
            error_logger:error_msg("Cannot delete ~p dets table. State was ~p", [{TableName, Index}, State]),
            {reply, {error, invalid_table}, State}
    end;
handle_call({parse_yaml, FileName, TableOptions, ParseFunction}, {FromPid, _}, State) ->
    WorkerPid = spawn(?MODULE, parse_yaml, [FileName, TableOptions, ParseFunction, FromPid, State]),
    {reply, WorkerPid, State}; %% return pid of worker
handle_call(state, _From, State) ->  %% dev option.
    {reply, State, State};
handle_call(_Request, _From, State) ->
    {reply, undefined, State}.
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.
%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({reopen, TablePath, {TableName, TableIndex}}, #{dets_tables := TablesMap}=State) -> 
    ok = dets:close({TableName, TableIndex}),
    {ok, Ref}= dets:open_file(TablePath), %% if dets file deleted during this time. there is something global wrong with usage of app
    TableMap =maps:get(TableName,TablesMap,#{}),
    IndexMap =maps:get(TableIndex,TableMap,#{}),
    {noreply, State#{dets_tables => TablesMap#{TableName => TableMap#{TableIndex => IndexMap#{ref => Ref, file => TablePath}}}}};
handle_info(_Info, State) ->
    {noreply, State}.
%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.
%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



-define(SIMPLE_PARSER_FUNTION, simple_parser).
-define(SIMPLE_PARSER, {?MODULE, ?SIMPLE_PARSER_FUNTION}).

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------

create_dets(TableName)-> 
    create_dets(TableName, []).
create_dets(TableName, Options)->
    gen_server:call(?MODULE, {create, dets, TableName, Options}).

gen_dets(TableName, Options, GenModule, GenFunction, GenOptiions)->
    gen_server:call(?MODULE, {create, gen_dets, TableName, Options, GenModule, GenFunction, GenOptiions}).

get_table({TableName, Index})->get_table(TableName, Index);
get_table(TableName)->
    gen_server:call(?MODULE, {get, dets, TableName}).
get_table(TableName, Index)->
    gen_server:call(?MODULE, {get, dets, TableName, Index}).


delete_dets({TableName, Index})->delete_dets(TableName, Index).
delete_dets(TableName, Index)->
    gen_server:call(?MODULE, {delete, dets, TableName, Index}).

-spec parse_yaml(string()) -> worker().

parse_yaml(FileName)->
    parse_yaml(FileName, [], ?SIMPLE_PARSER).

-type option() :: {access, dets:access()}
     | {auto_save, dets:auto_save()}
     | {estimated_no_objects, integer()}
     | {max_no_slots, dets:no_slots()}
     | {min_no_slots, dets:no_slots()}
     | {keypos, dets:keypos()}
     | {ram_file, boolean()}
     | {repair, boolean() | force}
     | {type, dets:type()}
     | {name, tablename()}
     | {file, file:name()}.

-type pase_fun() :: {module(), function()}.
-type worker() :: pid().
-type tablename() :: {string(), string() | integer()}.
-spec parse_yaml(string(), [option()] | pase_fun()) -> worker().

parse_yaml(FileName, TableOptions) when is_list(TableOptions)->
    parse_yaml(FileName, TableOptions, ?SIMPLE_PARSER);
parse_yaml(FileName, ParseFunction) when is_tuple(ParseFunction)->
    parse_yaml(FileName, [], ParseFunction).

-spec parse_yaml(string(), option(), pase_fun()) -> worker().

parse_yaml(FileName, TableOptions, ParseFunction) when is_tuple(ParseFunction)->
    gen_server:call(?MODULE, {parse_yaml, FileName, TableOptions, ParseFunction}).

-spec wait_parse_yaml(pid()) -> tablename() | {error, term()}.

wait_parse_yaml(Pid)->
    receive
        {yaml_parsed, Pid, TableName}-> TableName;
        {error, Pid, Error}->{error, Error}
    end.

-spec wait_parse_yaml(pid(), integer()) -> tablename() | {error, term()}.

wait_parse_yaml(Pid, Time) when is_pid(Pid)->
    receive
        {yaml_parsed, Pid, TableName}-> TableName;
        {error, Pid, Error}->{error, Error}
    after
        Time->{error, timeout}
    end.

-spec wait_parse_yaml_list([pid()]) -> [tablename()] | {error, term()}.

wait_parse_yaml_list(PidList) ->wait_parse_yaml_list(PidList, []).

wait_parse_yaml_list([], TableNames) ->TableNames;
wait_parse_yaml_list(PidList, TableNames)->
    receive
        {yaml_parsed, Pid, TableName}  ->
            case lists:member(Pid,PidList) of
                true-> 
                    error_logger:info_msg("~p ready", [TableName]),
                    wait_parse_yaml_list(lists:delete(Pid,PidList), [TableName|TableNames]);
                false->wait_parse_yaml(PidList, TableNames)
            end;
        {error, Pid, Error}->
            case lists:member(Pid,PidList) of
                true-> 
                    error_logger:info_msg("Error from ~p  - ~p", [Pid, Error]),
                    wait_parse_yaml_list(lists:delete(Pid,PidList), TableNames);
                false->wait_parse_yaml(PidList, TableNames)
            end
    end.



state()->
    gen_server:call(?MODULE,state).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------




-define(YAML_PARSING_OPTIONS, [{maps, true}]).

parse_yaml(_FilePath, TableOptions, {ParseModule, ParseFunction}, ReturnPid, #{sde_dir := SdeDir}=State)->
    _TableName = filename:basename(_FilePath, ".yaml"),
    FilePath = filename:join(SdeDir, _FilePath),
    {TableName, Dets} = create_dets(_TableName, TableOptions),
    case fast_yaml:decode_from_file(FilePath, ?YAML_PARSING_OPTIONS) of
        {ok, ParsedYaml}->
            apply(ParseModule, ParseFunction, [TableName, ParsedYaml]),
            TablePath = get_dets_file(TableName, Dets),
            ok = dets:sync(TableName),
            maps:get(pid, State, ReturnPid) ! {reopen, TablePath, TableName},
            ReturnPid ! {yaml_parsed, self(), TableName}, ok;
        Error->
            error_logger:error_msg("Cannot parse ~p yaml file (~p table). ~p", [FilePath, TableName, Error]),
            ReturnPid ! {error, self(), Error},
            throw(Error)
    end.

gen_dets(TableName, Options, GenModule, GenFunction, GenOptiions, ReturnPid, State)->
    {TableName, Dets} = create_dets(TableName, Options),
    TablePath = get_dets_file(TableName, Dets),
    maps:get(pid, State, ReturnPid) ! {reopen, TablePath, TableName},
    Ret = apply(GenModule, GenFunction, [TableName|GenOptiions]),
    ReturnPid ! Ret.

create_dets(_TableName, PrivDir, _Options, TablesMap)->
    {{BaseName, Index}=TableName, _FilePath, Options1} = proc_options(_Options, _TableName),
    {FilePath, Options} = case _FilePath of
        undefined -> 
            FilePath1 = ?MODULE:gen_table_path(PrivDir, TableName),
            {FilePath1, [{file, FilePath1} |Options1]};
        _->
            {_FilePath, Options1}
    end,
    {ok, TableName} = dets:open_file(TableName, Options),
    BaseMap = maps:get(BaseName, TablesMap, #{}),  %% already checked at stage of generating index
    {TableName, TablesMap#{BaseName => BaseMap#{Index => #{file => FilePath}}}}.

proc_options(Options, _TableName)->
     %% split common dets options. exept file option
    {ComOptions, _Rest} = proplists:split(Options,
        [access, auto_save, estimated_no_objects, max_no_slots, min_no_slots, keypos, ram_file, repair, type]
    ),
    Rest = lists:flatten(_Rest),
    TableName = proplists:get_value(name, Rest, _TableName),
    case proplists:get_value(file, Rest) of
        undefined->
            {TableName, undefined, lists:flatten(ComOptions)};
        FilePath->
            {TableName, FilePath, [{file, FilePath}|lists:flatten(ComOptions)]}
    end.


gen_table_path(Root, {Name,Index}) when is_list(Index)->
    filename:join(Root, lists:append([Name, "_", Index, ".dets"]));
gen_table_path(Root, {Name,Index}) when is_integer(Index)->
    filename:join(Root, lists:append([Name, "_", integer_to_list(Index), ".dets"])).

gen_tablename({BaseName,Index}, TablesMap)->
    {BaseName,Index};
gen_tablename(BaseName, TablesMap)->
    {BaseName, get_max_index(BaseName, TablesMap)+1}.
get_max_index(BaseName, TablesMap)->
    BaseMap = maps:get(BaseName, TablesMap, #{}),
    case maps:keys(BaseMap) of
        [] -> 0;
        KeysList -> lists:foldr(fun
            (IntKey,MaxKey) when is_integer(IntKey) and (IntKey > MaxKey) -> IntKey;
            (_NonIntKey,MaxKey)-> MaxKey
        end, 0, KeysList)
    end.

is_valid_tablename({TableName, TableIndex}=_Table, TablesMap)->
    case maps:get(TableIndex,maps:get(TableName, TablesMap, #{}), undefined) of
        undefined ->
            false;
        #{ref:=TableRef} ->
            dets:info(TableRef) =/= undefined
    end.

?SIMPLE_PARSER_FUNTION(Table, [YamlMap])->
    maps:fold(fun write_new_table/3, Table, YamlMap).

write_new_table(Key, Record, Table)->
    true = dets:insert_new(Table, {Key, Record}), Table.


delete_dets(TableName, TableIndex, TablesMap)->
    TableNameMap = maps:get(TableName, TablesMap, #{}),
    case maps:get(TableIndex, TableNameMap, undefined) of
        undefined ->
           {error, {invali_dets, TablesMap}};
        TableMap when is_map(TableMap) ->
            case maps:get(ref, TableMap, undefined) of
                undefined->
                    {error, {undef_ref, TableMap}};
                Ref ->
                    case dets:close(Ref) of
                        ok->
                            case maps:get(file, TableMap, undefined) of
                                undefined->
                                    {error, {undef_file, TableMap}};
                                FilePath ->
                                    case file:delete(FilePath) of
                                        ok->
                                            maps:remove(TableIndex, TableNameMap);
                                        {error, Reason}->
                                            {error, {unable_to_delete_file, Reason}}
                                    end
                            end;
                        {error, Reason}->
                            {error, {unable_to_close_dets, Reason}}
                    end
            end
    end.

reopen_dets({TableName, TableIndex}=_Table, TablePath, TablesMap)-> 
    case dets:open_file(TablePath) of
        {ok, TableRef}->
            TableNameMap = maps:get(TableName, TablesMap, #{}),
            {TableName, TablesMap#{TableName => TableNameMap#{TableIndex => #{file => TablePath, ref => TableRef}}}};
        Error->
            Error
    end.

get_dets_file({TableName, TableIndex}, DetsMap)->
    maps:get(file, maps:get(TableIndex, maps:get(TableName, DetsMap, #{}), #{}), undefined).

load_all_dets(PrivDir, _Map)->
    DetsFiles = filelib:wildcard( "*.dets", PrivDir),
    lists:foldr(fun(X, Map)-> 
        Table = parse_dets_tablepath(X, Map), 
        TablePath = filename:join(PrivDir, X), 
        element(2, reopen_dets(Table, TablePath, Map))
    end, _Map, DetsFiles).

parse_dets_tablepath(Path, Map)->
    BaseName = filename:basename(Path, ".dets"),
    case string:split(BaseName,"_") of
        [Name, []]-> %% no index in filename
            error_logger:error_msg("Cannot get index of ~p dets file.", [Path]),
            gen_tablename(Name,Map);
        [BaseName]-> %% no split '_' in filename
            error_logger:error_msg("Cannot get index of ~p dets file.", [Path]),
            gen_tablename(BaseName,Map);
        [Name, Index]->
            case catch list_to_integer(Index) of
                {'EXIT',_Reason}-> %% not int index
                    {Name, Index};
                IntIndex->
                    {Name, IntIndex}
            end
    end.
%% todo 
%% 