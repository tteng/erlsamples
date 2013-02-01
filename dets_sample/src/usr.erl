%%% File  :  usr.erl
%%% Description: Database api for subscribe DB
-module(usr).
-include("./templates/usr.hrl").
-export([start/0,           start/1,           
         stop/0,            init/2]).
-export([add_usr/3,         delete_usr/1,  
         set_service/3,     set_status/2,
         delete_disabled/0, lookup_id/1]).
-export([lookup_msisdn/1,   service_flag/2]).

-define(TIMEOUT, 30000).

start() ->
  start("usrTabFile").

start(FileName) ->
  register(?MODULE, spawn(?MODULE, init, [FileName, self()])),
  receive
    started -> ok
  after ?TIMEOUT ->
    {error, starting} 
  end.

stop() ->
  call(stop).

add_usr(PhoneNum, Id, Plan) when Plan == prepay; Plan == postpay ->
  call({add_usr, PhoneNum, Id, Plan}).

delete_usr(Id) ->
  call({delete_usr, Id}).

set_service(Id, Service, Flag) when Flag == true; Flag == false ->
  call({set_service, Id, Service, Flag}).

set_status(Id, Status) when Status == enabled;  Status == disabled ->
  call({set_status, Id, Status}).

delete_disabled() ->
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
init(FileName, Pid) ->
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

request({set_status, Id, Status}) ->
  case usr_db:lookup_id(Id) of
    {ok, Usr} ->
      usr_db:update_usr(Usr#usr{status=Status});
    {error, instance} ->
      {error, instance}
  end;

request(delete_disabled) ->
  usr_db:delete_disabled().
