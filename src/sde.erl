-module(sde).
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
start_link(ClientIndex) ->
    gen_server:start_link({via, sde_server_sup, ClientIndex}, ?MODULE, ClientIndex, []).
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------

start_link() ->
    ClientIndex = sde_server_sup:gen_name(),
    gen_server:start_link({via, sde_server_sup, ClientIndex}, ?MODULE, ClientIndex, []).
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
init(ClientIndex) ->
    {ok, #{id => ClientIndex, ets_tables => #{}}}.
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({create, ets, EtsTableName, DetsTableName, Options}, _From, #{ets_tables := EtsMap}=State) ->
    case maps:get(EtsTableName, EtsMap, undefined) of
        undefined->
            EtsRef = ets_from_dets(EtsTableName, DetsTableName, Options),
            {reply, EtsRef, State#{ets_tables => EtsMap#{EtsTableName => #{ref => EtsRef}}}};
        EtsTableNameMap when is_map(EtsTableNameMap)->
             {reply, {error, already_exists}, State}
    end;
handle_call({create, ets, DetsTableName, Options}, _From, #{ets_tables := EtsMap}=State) ->
    case maps:get(DetsTableName, EtsMap, undefined) of
        undefined->
            EtsRef = ets_from_dets(undefined, DetsTableName, lists:delete(named_table, Options)), %% named_table option can cause craches (ets names are only atoms)
            {reply, EtsRef, State#{ets_tables => EtsMap#{DetsTableName => #{ref => EtsRef}}}};
        EtsTableNameMap when is_map(EtsTableNameMap)->
            {reply, maps:get(ref, EtsTableNameMap, {error, undefined}), State} %% if already exist - retun ref
    end;
handle_call({ets,TableName}, _From, #{ets_tables := EtsMap}=State) ->
    case maps:get(TableName,EtsMap, undefined) of
        #{ref := Ref}->
            {reply, Ref, State};
        _Error->
             {reply, {error, _Error}, State}
    end;
handle_call({ets, TableName, ApplyModule, ApplyFun, ArgumentsTail}, _From, #{ets_tables := EtsMap}=State) ->
    case maps:get(TableName,EtsMap, undefined) of
        #{ref := Ref}->
            case catch apply(ApplyModule, ApplyFun, [Ref|ArgumentsTail]) of
                {'EXIT', Error}->
                    {reply, {error, Error}, State};
                Res->
                    {reply, Res, State}
            end;
        _Error->
             {reply, {error, _Error}, State}
    end;
handle_call(index, _From, #{id := Index}=State) ->  %% dev option.
    {reply, Index, State};
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



%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------

get_index(Client) when is_pid(Client)->
    gen_server:call(Client, index).
get_ets(Client,TableName)->
    gen_server:call({via, sde_server_sup, Client}, {ets, TableName}).
create_ets(Client,EtsTableName, DetsTableName, Options)->
    gen_server:call({via, sde_server_sup, Client}, {create, ets, EtsTableName, DetsTableName, Options}).
create_ets(Client, DetsTableName, Options)->
    gen_server:call({via, sde_server_sup, Client}, {create, ets, DetsTableName, Options}).
create_ets_infinite(Client,EtsTableName, DetsTableName, Options)->
    gen_server:call({via, sde_server_sup, Client}, {create, ets, EtsTableName, DetsTableName, Options}, infinite).
create_ets_infinite(Client, DetsTableName, Options)->
    gen_server:call({via, sde_server_sup, Client}, {create, ets, DetsTableName, Options}, infinite).
apply_ets(Client,TableName, ApplyModule, ApplyFun, ArgumentsTail)->
    gen_server:call({via, sde_server_sup, Client}, {ets, TableName, ApplyModule, ApplyFun, ArgumentsTail}).
start_client()->
    ClientIndex = sde_server_sup:gen_name(),
    ct:log("~p generated ~p name",[self(), ClientIndex]),
    ClientSpec = {ClientIndex,
        {?MODULE, start_link, [ClientIndex]},
        temporary , 7000, worker, [?MODULE]},
    {ok, _ClientPid} =supervisor:start_child(sde_server_sup, ClientSpec), 
    ClientIndex.
stop_client(ClientIndex)->
    sde_server_sup:unregister_name(ClientIndex),
    supervisor:terminate_child(sde_server_sup, ClientIndex).


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

-type option() :: term().
-type options_header() :: term().

-spec compile_options([option()], [options_header()])->[option()].

%% options example : [option1, {tuple_key, tuple_value}, option2]
%% valid options example : [option1, {tuple_key}]


compile_options(Options, ValidOptions)->
    {PropOptions, Rest} = lists:foldr(fun
        ({PropKey, PropValue}, {CompileOptions, Rest})-> 
            case proplists:is_defined(PropKey, ValidOptions) of
                true->
                    {[{PropKey, PropValue}|CompileOptions], Rest};
                false->
                    {CompileOptions, [{PropKey, PropValue}|Rest]}
            end;
        (Key, {CompileOptions, Rest})-> 
            case lists:member(Key, ValidOptions) of
                true->
                    {[Key|CompileOptions], Rest};
                false->
                    {CompileOptions, [Key|Rest]}
            end
    end, {[], []}, Options).

ets_from_dets(EtsTableName, DetsTableName, Options)->
    DetsRef = sde_server:get_table(DetsTableName),
    DetsType = dets:info(DetsRef, type),
    DetsKeypos = dets:info(DetsRef, keypos),
    {_Options,_Rest}=compile_options(Options, [public, protected, private, named_table, {write_concurrency}, {read_concurrency}, compressed]),
    Options1 = [DetsType, {keypos, DetsKeypos}|_Options],
    EtsRef = ets:new(EtsTableName, Options1),
    true = ets:from_dets(EtsRef, DetsRef),  %% todo - spawn_link for ets initialization with heir usage
    EtsRef.