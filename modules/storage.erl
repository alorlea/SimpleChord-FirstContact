%% Author: Alberto Lorente Leal, 
%% albll@kth.se
%% a.lorenteleal@gmail.com

-module(storage).

-export([create/0,add/3,lookup/2,merge/2,split/2]).

create() ->
	[].

add(Store,Key,Value) ->
	lists:sort([Store|{Key,Value}]).

lookup(Store,Key) ->
	 case lists:keysearch(Key, 1, Store) of
		 {value,{Key,Value}} ->
			 {Key,Value};
		 false->
			[]
	 end.

merge(Store1,Store2) ->
	lists:merge(Store1,Store2).

split(Nkey,Store) ->
	lists:partition(fun({Key,Value}) -> Key=<Nkey end, Store).
		 

