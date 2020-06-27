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
            dets_tables => load_all_dets(PrivDir, #{})}
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
handle_call({create, dets, _TableName, Options}, _From, #{priv_dir := PrivDir, dets_tables := TablesMap}=State) ->
    TableName = {_TableName, gen_index(_TableName, TablesMap)},  %% index for tables in case of scaling or version control
    FilePath = ?MODULE:gen_table_path(PrivDir, TableName),  %%  TODO add check for predefined filepath
    NewTablesMap = ?MODULE:create_dets(TableName, FilePath, Options, TablesMap),
    {reply, TableName, State#{dets_tables => NewTablesMap}};
handle_call({get, dets, TableName, Index}, _From, #{priv_dir := PrivDir, dets_tables := TablesMap}=State) ->
     Res = maps:get(ref,
            maps:get(Index, 
                maps:get(TableName, TablesMap, #{Index => #{ref => {error, invalid_tablename}}}),
            #{ref => {error, invalid_index}}), 
           {error, undefined}),
     {reply, Res, State};
handle_call({delete, dets, Table}, _From, #{priv_dir := PrivDir, dets_tables := TablesMap}=State) ->
    IsValidTble = is_valid_tablename(Table, TablesMap),
    if
        IsValidTble->
            case delete_dets(Table, TablesMap) of
                {error, Reason}->
                    {reply, {error, Reason}, State};
                NewTablesMap ->
                    {reply, true, State#{dets_tables => NewTablesMap}}
            end;
        true->
            error_logger:error_msg("Cannot delete ~p dets table. State was ~p", [Table, State]),
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

get_table(TableName, Index)->
    gen_server:call(?MODULE, {get, dets, TableName, Index}).

delete_dets(Table)->
    gen_server:call(?MODULE, {delete, dets, Table}).

parse_yaml(FileName)->
    gen_server:call(?MODULE, {parse_yaml, FileName, [], ?SIMPLE_PARSER}).
parse_yaml(FileName, TableOptions) when is_list(TableOptions)->
    gen_server:call(?MODULE, {parse_yaml, FileName, TableOptions, ?SIMPLE_PARSER});
parse_yaml(FileName, ParseFunction) when is_tuple(ParseFunction)->
    gen_server:call(?MODULE, {parse_yaml, FileName, [], ParseFunction}).
parse_yaml(FileName, TableOptions, ParseFunction) when is_tuple(ParseFunction)->
    gen_server:call(?MODULE, {parse_yaml, FileName, TableOptions, ParseFunction}).

state()->
    gen_server:call(?MODULE,state).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

-define(YAML_PARSING_OPTIONS, [{maps, true}]).

parse_yaml(_FilePath, TableOptions, {ParseModule, ParseFunction}, ReturnPid, #{sde_dir := SdeDir}=State)->
    TableName = filename:basename(_FilePath, ".yaml"),
    FilePath = filename:join(SdeDir, _FilePath),
    Dets = ?MODULE:create_dets(TableName, TableOptions),
    error_logger:info_msg("Parsing ~p yaml file ....", [FilePath]),
    case fast_yaml:decode_from_file(FilePath, ?YAML_PARSING_OPTIONS) of
        {ok, ParsedYaml}->
            error_logger:info_msg("~p file parsed, writing dets table...", [FilePath]),
            apply(ParseModule, ParseFunction, [Dets, ParsedYaml]),
            error_logger:info_msg("~p writen as ~p dets table. Closing table...", [FilePath, Dets]),
            maps:get(pid, State, ReturnPid) ! {reload, TableName},
            ok;
        Error->
            error_logger:error_msg("Cannot parse ~p yaml file. ~p", [FilePath, Error]),
            ReturnPid ! {error, Error},
            throw(Error)
    end.

create_dets({BaseName, Index}=TableName, FilePath, Options, TablesMap)->
    {ok, TableName} = dets:open_file(TableName, [{file, FilePath} |Options]),
    BaseMap = maps:get(BaseName, TablesMap, #{}),  %% already checked at stage of generating index
    TablesMap#{BaseName => BaseMap#{Index => #{file => FilePath}}}.

gen_table_path(Root, {Name,Index})->
    filename:join(Root, lists:append([Name, "_", integer_to_list(Index), ".dets"])).

gen_index(BaseName, TablesMap)->
    BaseMap = maps:get(BaseName, TablesMap, #{}),
    case maps:keys(BaseMap) of
        [] -> 0;
        KeysList -> lists:last(lists:usort(KeysList))+1  %% assume that index is integer
    end.

is_valid_tablename({TableName, TableIndex}=Table, TablesMap)->
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


delete_dets({TableName, TableIndex}=Table, TablesMap)->
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

reopen_dets({TableName, TableIndex}=Table, TablePath, TablesMap)-> 
    case dets:open_file(TablePath) of
        {ok, TableRef}->
            TableNameMap = maps:get(TableName, TablesMap, #{}),
            {TableName, TablesMap#{TableName => TableNameMap#{TableIndex => #{file => TablePath, ref => TableRef}}}};
        Error->
            Error
    end.

load_all_dets(PrivDir, _Map)->
    DetsFiles = filelib:wildcard( "*.dets", PrivDir),
    lists:foldr(fun(X, Map)-> 
        Table = parse_dets_tablepath(X, Map), 
        TablePath = filename:join(PrivDir, X), 
        element(2, reopen_dets(Table, TablePath, Map))
    end, _Map, DetsFiles).

parse_dets_tablepath(Path, Map)->
    BaseName = filename:basename(Path, ".dets"),
    [Name, Index]=string:split(BaseName,"_"),
    case catch list_to_integer(Index) of
        {'EXIT',Reason}->
            {Name, gen_index(Name, Map)};
        IntIndex->
            {Name, IntIndex}
    end.
%% todo 
%% 