%% Author: Alberto Lorente Leal, 
%% albll@kth.se
%% a.lorenteleal@gmail.com
%% Created: 04/10/2011
%% Description: Based on node.erl, chord node with storage module
%% added the extra messages
-module(node2).


-export([start/1,start/2]).

-define(Stabilize,1000).
-define(Timeout,5000).

start(Id) ->
	start(Id, nil).

start(Id, Peer) ->
	timer:start(),
	spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
	Predecessor = nil,
	{ok, Successor} = connect(Id, Peer),
	schedule_stabilize(),
	node(Id, Predecessor, Successor,storage:create()).

connect(Id, nil) ->
		{ok, {Id,self()}};
connect(Id, Peer) ->
		Qref = make_ref(),
		Peer ! {key, Qref, self()},
		receive
			{Qref, Skey} ->
					{ok,{Skey,Peer}}
			after ?Timeout ->
				io:format("Time out: no response~n",[])
		end.

node(Id, Predecessor, Successor,Store) ->
	receive
		{key, Qref, Peer} ->
			Peer ! {Qref, Id},
			node(Id, Predecessor, Successor,Store);
		
		{notify, New} ->
			{Pred,NewStore} = notify(New, Id, Predecessor,Store),
			node(Id, Pred, Successor,NewStore);
		
		{request, Peer} ->
			request(Peer, Predecessor),
			node(Id, Predecessor, Successor,Store);
	
		{status, Pred} ->
			Succ = stabilize(Pred, Id, Successor),
			%%io:format("Sucessor:~p Predecesor:~p Id: ~p Store:~p~n", [Succ,Predecessor,Id,Store]),
			node(Id, Predecessor, Succ,Store);
			
		stabilize ->
			stabilize(Successor),
			node(Id, Predecessor, Successor,Store);
		
		probe ->
			create_probe(Id,Successor,Store),
			node(Id, Predecessor, Successor,Store);
		
		{probe, Id, Nodes, T} ->
			remove_probe(T, Id, Nodes),
			node(Id, Predecessor, Successor,Store);
		
		{probe, Ref, Nodes, T} ->
			forward_probe(Ref, Id, T, Nodes, Successor,Store),
			node(Id, Predecessor, Successor,Store);
			
		%%Extra message compared to original code
		{add, Key, Value, Qref, Client} ->
			Added = add(Key, Value, Qref, Client,
								Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Added);
		%%Extra message compared to original code
		{lookup, Key, Qref, Client} ->
			lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Store);
		
		{handover, Elements} ->
			Merged = storage:merge(Store, Elements),
			node(Id, Predecessor, Successor, Merged)
		
	end.

%% This stabilize function was implement by ourselves.
%% 
stabilize(Pred, Id, Successor) ->
	{Skey, Spid} = Successor,
	
	case Pred of
		nil ->
			Spid ! {notify, {Id,self()}},
			Successor;
		{Id, _} ->
			Successor;
		{Skey, _} ->
			Spid ! {notify, {Id,self()}},
			Successor;
		{Xkey, Xpid} ->
			case key:between(Xkey, Id, Skey) of
				true ->
					Xpid ! {request, self()},
					{Xkey,Xpid};

				false ->
					Spid ! {notify, {Id,self()}},
					Successor
			end
	end.

schedule_stabilize() ->
	timer:send_interval(?Stabilize, self(), stabilize).



stabilize({_, Spid}) ->
	Spid ! {request, self()}.

request(Peer, Predecessor) ->
	case Predecessor of
	nil ->
		Peer ! {status, nil};
		
	{Pkey, Ppid} ->
		Peer ! {status, {Pkey, Ppid}}
end.

%% This notify function was implemented by ourselves.
%%

notify({Nkey, Npid}, Id, Predecessor,Store) ->
	case Predecessor of
		nil ->
			Keep=handover(Store,Nkey,Npid),
			{{Nkey, Npid},Keep};
		{Pkey, _} ->
			case key:between(Nkey, Pkey, Id) of
				true ->
					Keep = handover(Store,Nkey,Npid),
					{{Nkey,Npid},Keep};
				false ->
					{Predecessor,Store}
			
			end
	end.
	
%% new function we added compared with the original node code
%% add new element
add(Key, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
	case key:between(Key, Pkey, Id) of
		true ->
			Client ! {Qref, ok},
			storage:add(Store,Key,Value);
		false ->
			Spid ! {add,Key,Value,Qref,Client}
	end.
	
%% new function we added compared with the original node code
%% lookup procedure to search on the chord ring	
lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store) ->
	case key:between(Key, Pkey, Id) of
		true ->
			Result = storage:lookup(Key, Store),
			Client ! {Qref, Result};
		false ->
			{_, Spid} = Successor,
			Spid ! {lookup, Key, Qref, Client}
	end.
	
%% new function to handover keys to new nodes
handover(Store, Nkey, Npid) ->
	{Leave,Keep}= storage:split(Nkey,Store),
	Npid ! {handover, Leave},
	Keep.

create_probe(Id, {_,Spid},Store)->
	Spid! {probe,Id, [{Id,Store}], now()}.

forward_probe(Ref,Id,T,Nodes, {_,Spid},Store)->
	Spid !{probe,Ref, [{Id,Store}| Nodes],T}.

remove_probe(T,Id,Nodes)->
	Time = timer:now_diff(now(),T),
	io:format("Node(~w): time=~.2fms, stores=~w~n", [Id, Time/1000,Nodes]).