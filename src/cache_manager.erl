%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. May 2021 22:16
%%%-------------------------------------------------------------------
-module(cache_manager).
-author("pandey").

%% API
-export([get_from_cache/3,put_in_cache/1]).

-spec get_from_cache(atom(),atom(),vectorclock:vectorclock()) -> list().
get_from_cache( ObjectKey,Type,MaximumSnapshotTime)->
  gen_server:call(cache_manager_server,{get_from_cache, ObjectKey,Type,MaximumSnapshotTime }).

-spec put_in_cache({term(), antidote_crdt:typ(), vectorclock:vectorclock()}) -> boolean().
put_in_cache(Data)->
  gen_server:call(cache_manager_server,{put_in_cache, Data}).
