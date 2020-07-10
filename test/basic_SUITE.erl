-module(basic_SUITE).

-include_lib("mixer/include/mixer.hrl").
-include_lib("stdlib/include/assert.hrl").
-mixin([{ktn_meta_SUITE, [dialyzer/1]}]).

-compile(export_all).

all()->
  [
    state_test,
    parse_yaml_test,
    race_test
  ].

-type config() :: [{atom(), term()}].

-define(SDE_DIR,  filename:join([code:lib_dir(sde_server),"test","test_sde"])).
-define(PRIV_DIR, filename:join([code:lib_dir(sde_server),"test","test_priv"])).

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
  random:seed(now()),
  ok = application:set_env(sde_server, sde_dir, ?SDE_DIR),
  ok = application:set_env(sde_server, priv_dir, ?PRIV_DIR),
  clean_dir_all(?PRIV_DIR),
  {ok, StartedLists} = application:ensure_all_started(sde_server),
  [{application, sde_server}| Config].

-spec end_per_suite(config()) -> term() | {save_config, config()}.
end_per_suite(Config) ->
  application:stop(sde_server),
  PrivDir = application:get_env(sde_server, priv_dir, "/test"),
  clean_dir_all(PrivDir),
  Config.

state_test(_Config)->
  Map = sde_server:state(),
  ?assert(is_map(Map)),
  ?assertEqual(?SDE_DIR, maps:get(sde_dir, Map)),
  ?assertEqual(?PRIV_DIR, maps:get(priv_dir, Map)).
  
-define(LARGE_FILE, "typeIDs.yaml").
-define(BAG_FILE, "dogmaAttributes.yaml").

parse_yaml_test(_Config)->
  Pid = sde_server:parse_yaml(?LARGE_FILE),
  ?assert(is_pid(Pid)),
  
  Pid2 = sde_server:parse_yaml(?BAG_FILE, [{name, {"foo","bar"}}, {type, bag}]),
  ?assert(is_pid(Pid2)),
  [TableName1,TableName2]=sde_server:wait_parse_yaml_list([Pid,Pid2]),
  ?assertNotEqual(TableName1, TableName2).

race_test(_Config)->
  PidList = lists:map(fun(Index)->
      sde_server:parse_yaml(?BAG_FILE, [{name, {"race",Index}}, {type, bag}])
    end, lists:seq(1,100)),
  BaseRef = sde_server:get_table("foo","bar"),
  BaseNoKeys = dets:info(BaseRef, no_keys),
  TableNames = sde_server:wait_parse_yaml_list(PidList),
  %% check all created tables for 
  ?assertEqual([], lists:foldr(fun(Index, Acc)->
    Ref = sde_server:get_table("race", Index),
    case dets:info(Ref, no_keys) of
      BaseNoKeys->Acc;
      InvalidValue-> [{Index, InvalidValue}|Acc]
  end end, [], lists:seq(1,100))).


clean_dir_all(Dir)->
  {ok, FileList} = file:list_dir_all(Dir),
  lists:foreach(fun(X)-> file:delete(filename:join(Dir, X)) end, FileList).