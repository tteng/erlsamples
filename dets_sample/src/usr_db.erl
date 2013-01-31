%%% File  :  usr_db.erl
%%% Description: Database api for subscribe DB
-module(usr_db).
-include("./templates/usr.hrl").
-export([start/0,           start/1,           
         stop/0,            init/2]).
-export([add_usr/3,         delete_usr/1,  
         set_service/3,     set_status/2,
         delete_disabled/0, lookup_id/1]).
-export([create_tables/1,   close_tables/0]).
-export([lookup_msisdn/1,   service_flag/2]).

-define(TIMEOUT, 30000).

start() ->
  start("usrDb").

start(FileName) ->
  register(?MODULE, spawn(?MODULE, init, [FileName, self()])),
  receive
    started -> ok
  after ?TIMEOUT ->
    {error, starting} 
  end.

stop() ->
  call(stop).

add_usr(PhoneNum, Id, Plan) when Plan == prepay; Plah == postpay ->
  call({add_usr, PhoneNum, Id, Plan}).

delete_usr(Id) ->
  call({delete_usr, Id}).

set_service(Id, Service, Flag) when Flag == true; Flag == false ->
  call({set_service, Id, Service, Flag}).

set_status(Id, Status) when Status == enabled,  Status == disabled ->
  call({set_status, Id, Status}).

dele_disabled() ->
  call(delete_disabled).

lookup_id(Id) ->
  usr_db:lookup_id(Id).

%%Service API
lookup_msisdn(PhoneNo) ->
  usr_db:lookup_msisdn(PhoneNo).

service_flag(PhoneNo, Service) ->
  case usr_db:lookup_msisdn(PhoneNo) of 
    {ok, #usr{services=Services, status=enabled}} ->
      lists:member(Service, Services);
    {ok, #usr{status=disabled}} ->
      {error, disabled};
    {error, Reason} ->
      {error, Reason}
  end.  

%%Message Functions
call(Request) ->
  Ref = make_ref(),
  ?MODULE ! {request, {self(), Ref}, Request},
  receive
    {reply, Ref, Reply} -> Reply
  after  ?TIMEOUT -> 
    {error, timeout}
  end.

reply({From, Ref}, Reply) ->
  From ! {reply, Ref, Reply}.

%%Internal Server Functions
init(FileNmae, Pid) ->
  usr_db:create_tables(FileName),
  usr_db:restore_backup(),
  Pid ! started,
  %% What happend if send msg with ?MODULE ! started
  loop().

loop() ->
  receive
    {request, From, stop} -> 
      reply(From, usr_db:close_tables());
    {request, From ,Request} ->
      Reply = request(Request),
      reply(From, Reply),
      loop()
  end.

%%Handling Client Requests
request({add_usr, PhoneNo, Id, Plan}) ->
  usr_db:add_usr(#usr{msisdn=PhoneNo, id=Id, plan=Plan});

request({delete_usr, Id}) ->
  usr_db:delete_usr(Id);

request({set_service, Id, Service, Flag}) ->
  case usr_db:lookup_id(Id) of
    {ok, Usr} ->
      Services = lists:delete(Service, Usr#usr.services),
      NewServices = case Flag of
        true  -> [Service | Services];
        false -> Services
      end,
      usr_db:update_usr(Usr#usr{services=NewServices});
    {error, instance} ->
      {error, instance}
  end;

request({set_status, Id, Status) ->
  case usr_db:lookup_id(Id) of
    {ok, Usr} ->
      usr_db:update_usr(Usr#usr{status=Status});
    {error, instance} ->
      {error, instance}
  end;

request(delete_disabled) ->
  usr_db:delete_disabled().

%%DB functions
create_tables(FileName) ->
  ets:new(usrIndex, [named_table]),
  ets:new(usrRam, [named_table, {keypos, #usr.msisdn}]),
  dets:open_file(usrDisk, [{file, FileName}, {keypos, #usr.msisdn}]).

close_tables() ->
  ets:delete(usrRam),
  ets:delete(usrIndex),
  dets:close_file(usrDisk).

lookup_id(Id) ->
  case get_index(Id) of
    {ok, PhoneNo} -> lookup_msisdn(PhoneNo);
    {error, instance} -> {error, instance}
  end.

add_usr(#usr{msisdn=PhoneNo, id=Id} = Usr) ->
  ets:insert(usrIndex, {Id, PhoneNo}),
  update_usr(Usr).

update_usr(Usr) ->
  ets:insert(usrRam, Usr),
  dets:insert(usrDisk, Usr),
  ok.

lookup_msisdn(PhoneNo) ->
  case ets:lookup(usrRam, PhoneNo) of
    [Usr] -> {ok, Usr};
    []    -> {error, instance}
  end.

get_index(Id) ->
  case ets:lookup(usrIndex, Id) of 
    [{Id, PhoneNo}] -> {ok, PhoneNo};
    []                  -> {error, instance}
  end

restore_backup() ->
  Insert = fun(#usr{msisdn=PhoneNo, id=Id} = Usr) ->
    ets:insert(usrRam, Usr),
    ets:insert(usrIndex, {Id, PhoneNo}),
    continue
  end,
  dets:traverse(usrDisk, Insert).

delete_disabled() ->
  ets:save_fixtable(usrRam, true),
  catch loop_delete_disabled(ets:first(usrRam)),
  ets:safe_fixtable(usrRam, false),
  ok.

loop_delete_disabled('$end_of_file') ->
  ok;
loop_delete_disabled(PhoneNo) ->
  case ets:lookup(usrRam, PhoneNo) of 
    [#usr{status=disabled, id=Id}] ->
      delete_usr(PhoneNo, Id);
    _ -> ok
  end,
  loop_delete_disabled(ets:next(usrRam, PhoneNo)).

delete_usr(PhoneNo, Id) ->
  ets:delete(usrIndex, Id), 
  ets:delete(usrRam, PhoneNo), 
  dets:delete(usrDisk, PhoneNo).
