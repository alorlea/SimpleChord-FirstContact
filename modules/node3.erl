%% Author: Alberto Lorente Leal, 
%% albll@kth.se
%% a.lorenteleal@gmail.com
%% Created: 05/10/2011
%% Description: based on node2 code, we add failure detection and reorganization
%% We add in four place monitor reference and 2 places we demonitor
-module(node3).

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
	node(Id, Predecessor, Successor,storage:create(),nil).

connect(Id, nil) ->
		{ok, {Id, monitor(self()), self()}};

connect(Id, Peer) ->
		Qref = make_ref(),
		Peer ! {key, Qref, self()},
		receive
			{Qref, Skey} ->
					{ok,{Skey, monitor(Peer),Peer}}
			after ?Timeout ->
				io:format("Time out: no response~n",[])
		end.

node(Id, Predecessor, Successor,Store, Next) ->
	receive
		{key, Qref, Peer} ->
			Peer ! {Qref, Id},
			node(Id, Predecessor, Successor,Store,Next);
		
		{notify, New} ->
			{Pred,NewStore} = notify(New, Id, Predecessor,Store),
			node(Id, Pred, Successor,NewStore,Next);
		
		{request, Peer} ->
			request(Peer, Predecessor,Successor),
			node(Id, Predecessor, Successor,Store,Next);
	
		{status, Pred,Nx} ->
			{Succ,Nxt} = stabilize(Pred, Id, Successor, Nx),
			io:format("Sucessor:~p Predecesor:~p Id: ~p Store:~p~n", [Succ,Predecessor,Id,Store]),
			node(Id, Predecessor, Succ,Store,Nxt);
			
		stabilize ->
			stabilize(Successor),
			node(Id, Predecessor, Successor,Store,Next);
		
		probe ->
			create_probe(Successor),
			node(Id, Predecessor, Successor,Store,Next);
		
		{probe, Id, Nodes, T} ->
			remove_probe(T, Nodes),
			node(Id, Predecessor, Successor,Store,Next);
		
		{probe, Ref, Nodes, T} ->
			forward_probe(Ref, T, Nodes, Successor),
			node(Id, Predecessor, Successor,Store,Next);
		
		{add, Key, Value, Qref, Client} ->
			Added = add(Key, Value, Qref, Client,
								Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Added,Next);

		{lookup, Key, Qref, Client} ->
			lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Store,Next);
		
		{handover, Elements} ->
			Merged = storage:merge(Store, Elements),
			node(Id, Predecessor, Successor, Merged,Next)
		
	end.

%% This stabilize function was implement by ourselves.
%% 
stabilize(Pred, Id, Successor,Nx) ->
	{Skey,Sref, Spid} = Successor,
	
	case Pred of
		nil ->
			Spid ! {notify, {Id,self()}},
			{Successor,Nx};
		{Id, _} ->
			{Successor,Nx};
		{Skey, _} ->
			Spid ! {notify, {Id,self()}},
			{Successor,Nx};
		{Xkey, Xpid} ->
			case key:between(Xkey, Id, Skey) of
				true ->
					Xpid ! {request, self()},
					demonitor(Sref),
					{{Xkey,monitor(Xpid), Xpid},{Skey,Spid}};

				false ->
					Spid ! {notify, {Id,self()}},
					{Successor,Nx}
			end
	end.

schedule_stabilize() ->
	timer:send_interval(?Stabilize, self(), stabilize).



stabilize({_, Spid}) ->
	Spid ! {request, self()}.

request(Peer, Predecessor,MySuccesor) ->
	{Skey,_, Spid}=MySuccesor,
	case Predecessor of
	nil ->
		Peer ! {status, nil, {Skey,Spid}};
		
	{Pkey,_, Ppid} ->
		Peer ! {status, {Pkey, Ppid}, {Skey,Spid}}
end.

%% This notify function was implemented by ourselves.
%%

notify({Nkey, Npid}, Id, Predecessor,Store) ->
	{Pkey,Pref,Pid} = Predecessor,
	case Predecessor of
		nil ->
			Keep=handover(Store,Nkey,Npid),
			{{Nkey,monitor(Npid), Npid},Keep};
		{Pkey,_, _} ->
			case key:between(Nkey, Pkey, Id) of
				true ->
					Keep = handover(Store,Nkey,Npid),
					demonitor(Pref),
					{{Nkey,monitor(Npid), Npid},Keep};
				false ->
					{Predecessor,Store}
			
			end
	end.

add(Key, Value, Qref, Client, Id, {Pkey, _}, {_, Spid}, Store) ->
	case key:between(Key, Pkey, Id) of
		true ->
			Client ! {Qref, ok},
			storage:add(Store,Key,Value);
		false ->
			Spid ! {add,Key,Value,Qref,Client}
		end.

lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store) ->
	case key:between(Key, Pkey, Id) of
		true ->
			Result = storage:lookup(Key, Store),
			Client ! {Qref, Result};
		false ->
			{_, Spid} = Successor,
			Spid ! {lookup, Key, Qref, Client}
	end.

handover(Store, Nkey, Npid) ->
	{Keep,Leave}= storage:split(Nkey,Store),
	Npid ! {handover, Leave},
	Keep.

create_probe(Sucessor)->
	{Skey,Spid}=Sucessor,
	Spid! {probe,self(),[self()],erlang:now()}.

forward_probe(Ref,T,Nodes, Succesor)->
	{Skey,Spid}=Succesor,
	Spid !{probe,Ref, [Nodes|self()],timer:now_diff(T, erlang:now())}.
remove_probe(T,Nodes)->
	io:format("Time: ~p, Nodes: ~p",[T,Nodes]).

monitor(Pid) ->
	erlang:monitor(process, Pid).

demonitor(nil)->
	ok;
demonitor(Pid)->
	erlang:monitor(Pid, [flush]).