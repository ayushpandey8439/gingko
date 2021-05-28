%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------
-define(TABLE_CONCURRENCY, {read_concurrency,true}).

-module(cache_manager).
-author("pandey").

%% API
-export([create_cache/1,read_from_cache/2]).

-spec create_cache(term()) -> ets:tab().
create_cache(CacheIdentifier)->
  ets:new(CacheIdentifier,[named_table,?TABLE_CONCURRENCY]).

%%-spec read_from_cache(term(),term())
read_from_cache(CacheIdentifier, ObjectKey)->
  ets:lookup(CacheIdentifier,ObjectKey).

