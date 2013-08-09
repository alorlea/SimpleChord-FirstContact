%% Author: Alberto Lorente Leal, 
%% albll@kth.se
%% a.lorenteleal@gmail.com
%% Description: A representation of a Chord node
-module(node).


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
	node(Id, Predecessor, Successor).

node(Id, Predecessor, Successor) ->
	receive
		{key, Qref, Peer} ->
			Peer ! {Qref, Id},
			node(Id, Predecessor, Successor);
		
		{notify, New} ->
			Pred = notify(New, Id, Predecessor),
			node(Id, Pred, Successor);
		
		{request, Peer} ->
			request(Peer, Predecessor),
			node(Id, Predecessor, Successor);
	
		{status, Pred} ->
			Succ = stabilize(Pred, Id, Successor),
			io:format("Sucessor:~p Predecesor:~p Id: ~p~n", [Succ,Predecessor,Id]),
			node(Id, Predecessor, Succ);
		
		stabilize ->
			stabilize(Successor),
			node(Id, Predecessor, Successor);
		
		probe ->
			create_probe(Successor),
			node(Id, Predecessor, Successor);
		
		{probe, Id, Nodes, T} ->
			remove_probe(T, Nodes),
			node(Id, Predecessor, Successor);
		
		{probe, Ref, Nodes, T} ->
			forward_probe(Ref, T, Nodes, Successor),
			node(Id, Predecessor, Successor)
		
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

notify({Nkey, Npid}, Id, Predecessor) ->
	case Predecessor of
		nil ->
			{Nkey, Npid};
		{Pkey, _} ->
			case key:between(Nkey, Pkey, Id) of
				true ->
					{Nkey,Npid};
				false ->
					Predecessor
			
			end
	end.

%% Connect was also implemented by us.	
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

create_probe(Sucessor)->
	{Skey,Spid}=Sucessor,
	Spid! {probe,self(),[self()],erlang:now()}.

forward_probe(Ref,T,Nodes, Succesor)->
	{Skey,Spid}=Succesor,
	Spid !{probe,Ref, [Nodes|self()],timer:now_diff(T, erlang:now())}.
remove_probe(T,Nodes)->
	io:format("Time: ~p, Nodes: ~p",[T,Nodes]).