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
-export([get_from_cache/4,put_in_cache/2]).

-spec get_from_cache(term(),atom(),atom(),vectorclock:vectorclock()) -> list().
get_from_cache(CacheIdentifier, ObjectKey,Type,MaximumSnapshotTime)->
  io:format("Called get from cache~n"),
  gen_server:call(cache_manager_server,{get_from_cache, CacheIdentifier, ObjectKey,Type,MaximumSnapshotTime }).

-spec put_in_cache(term(),{term(), antidote_crdt:typ(), vectorclock:vectorclock()}) -> boolean().
put_in_cache(CacheIdentifier,Data)->
  gen_server:call(cache_manager_server,{put_in_cache,CacheIdentifier, Data}).
