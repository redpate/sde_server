-module(client_SUITE).

-include_lib("mixer/include/mixer.hrl").
-include_lib("stdlib/include/assert.hrl").
-mixin([{ktn_meta_SUITE, [dialyzer/1]}]).

-compile(export_all).

all()->
  [
    client_start_test,
    ets_test,
    race_test
  ].

-type config() :: [{atom(), term()}].

-define(SDE_DIR,  filename:join([code:lib_dir(sde_server),"test","test_sde"])).
-define(PRIV_DIR, filename:join([code:lib_dir(sde_server),"test","test_priv"])).

-spec init_per_suite(config()) -> config().

-define(LARGE_FILE, "typeIDs.yaml").
-define(BAG_FILE, "dogmaAttributes.yaml").

init_per_suite(Config) ->
  random:seed(now()),
  ok = application:set_env(sde_server, sde_dir, ?SDE_DIR),
  ok = application:set_env(sde_server, priv_dir, ?PRIV_DIR),
  {ok, StartedLists} = application:ensure_all_started(sde_server),
  [{application, sde_server}| Config].

-spec end_per_suite(config()) -> term() | {save_config, config()}.
end_per_suite(Config) ->
  application:stop(sde_server),
  PrivDir = application:get_env(sde_server, priv_dir, "/test"),
  clean_dir_all(PrivDir),
  Config.
  
client_start_test(_Config)->
  ClientIndex = sde:start_client(),
  ?assert(is_pid(sde_server_sup:whereis_name(ClientIndex))),
  sde:stop_client(ClientIndex).
  

ets_test(_Config)->
  ClientIndex = sde:start_client(),
  ct:log("~p created", [ClientIndex]),
  Pid1 = sde_server:parse_yaml(?BAG_FILE, [{name, {"ets","test"}}, {type, bag}]),
  BagTableName = sde_server:wait_parse_yaml(Pid1),
  EtsRef = sde:create_ets(ClientIndex, BagTableName, []),
  ?assert(not is_tuple(EtsRef)), %% returned not error tuple
  Ets = sde:get_ets(ClientIndex, BagTableName),
  ?assert(not is_tuple(Ets)), %% returned not error tuple
  ?assertEqual(EtsRef, Ets),
  Res1 = ets:info(Ets),
  Res2 = sde:apply_ets(ClientIndex,{"ets","test"},?MODULE,apply_info,[]),
  ?assertEqual(Res1, Res2),
  sde:stop_client(ClientIndex),
  ct:log("~p stopped", [ClientIndex]).

race_test(_Config)-> %% not rly accurate test. clients should not be used as mas-spawning proceses
  Pid = sde_server:parse_yaml(?BAG_FILE, [{name, {"ets","race"}}, {type, bag}]),
  BagTableName = sde_server:wait_parse_yaml(Pid),
  PidList= lists:map(fun(Index)->
    ct:log("~p spawned", [Index]),
    spawn_link(?MODULE, ets_test_spawn, [self(),Index,BagTableName]) end, lists:seq(1,20)),
  ?assertEqual([],sde_server:wait_parse_yaml_list(PidList)).  %% using wait_parse_yaml_list fucntion to ensure all processes completed 

ets_test_spawn(RetPid, _Index, BagTableName)->
  ClientIndex = sde:start_client(),
  ct:log("~p created", [ClientIndex]),
  EtsRef = sde:create_ets(ClientIndex, BagTableName, []),
  ?assert(not is_tuple(EtsRef)), %% returned not error tuple
  Ets = sde:get_ets(ClientIndex, BagTableName),
  ?assert(not is_tuple(Ets)), %% returned not error tuple
  ?assertEqual(EtsRef, Ets),
  Res1 = ets:info(Ets),
  Res2 = sde:apply_ets(ClientIndex,BagTableName,?MODULE,apply_info,[]),
  ?assertEqual(Res1, Res2),
  sde:stop_client(ClientIndex),
  ct:log("~p stopped", [ClientIndex]),
  RetPid ! {error, self(), {_Index, ClientIndex, exit}}.

clean_dir_all(Dir)->
  {ok, FileList} = file:list_dir_all(Dir),
  lists:foreach(fun(X)-> file:delete(filename:join(Dir, X)) end, FileList).


apply_info(Ets)->
    ets:info(Ets).