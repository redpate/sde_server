%%%-------------------------------------------------------------------
%% @doc sde_server top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(sde_server_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-export([register_name/2, unregister_name/1, whereis_name/1, send/2, gen_name/0]).

-define(CLIENTS_TABLE, sde_server_clients).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    ets:new(?CLIENTS_TABLE, [set, named_table, public, {read_concurrency,true}]),
    ets:insert_new(?CLIENTS_TABLE, {counter, 0}),
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [
        #{id => sde_controller, start => {sde_server, start_link,[]}}
    ],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions


%%% gproc is too much for such simple proc registration

register_name(Name, Pid)->
    case ets:lookup(?CLIENTS_TABLE, Name) of
        []->
            case ets:insert_new(?CLIENTS_TABLE, {Name, Pid}) of
                true->yes;
                false->no
            end;
        [{Name, undefined}]->
            ets:insert(?CLIENTS_TABLE, {Name, Pid}), yes;
        _Error->
            no
    end.

unregister_name(Name)->
    ets:delete(?CLIENTS_TABLE, Name).

whereis_name(Name)->
    case ets:lookup(?CLIENTS_TABLE, Name) of
        []->undefined;
        [{Name, Pid}]->Pid
    end.

send(Name, Msg)->
    case whereis_name(Name) of
        undefined -> {error, undefined_name};
        Pid -> Pid ! Msg
    end.

gen_name()->
    NewIndex = ets:update_counter(?CLIENTS_TABLE, counter, 1),
    case ets:insert_new(?CLIENTS_TABLE, {NewIndex, undefined}) of
            true->
                NewIndex;
            false->
                {error, failed_to_insert}
    end.

%% todo: counter reduction if ets:last return much less then counter