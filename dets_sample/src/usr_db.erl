%%% File  :  usr_db.erl
%%% Description: Database api for subscribe DB
-module(usr_db).
-include("./templates/usr.hrl").
-export([create_tables/1,   close_tables/0]).
-export([lookup_id/1,       lookup_msisdn/1,
         update_usr/1,      add_usr/1,
         restore_backup/0,  delete_disabled/0
       ]).

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
  end.

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
