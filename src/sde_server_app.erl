%%%-------------------------------------------------------------------
%% @doc sde_server public API
%% @end
%%%-------------------------------------------------------------------

-module(sde_server_app).

-behaviour(application).

-export([start/2, stop/1, get_env/1, get_env/2]).

-define(APPLICATION, sde_server).

start(_StartType, _StartArgs) ->
    sde_server_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

get_env(Key, DefaultKey)->
    application:get_env(?APPLICATION, Key, application:get_env(?APPLICATION, DefaultKey, undefined)).

get_env(Key)->
    application:get_env(?APPLICATION, Key, false).