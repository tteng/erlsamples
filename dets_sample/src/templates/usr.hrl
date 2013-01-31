%%% File: usr.hrl
%%% Description: Include file for user db

-record(usr, 
            {
              msisdn,  %int()
              id,     %term()
              status = enabled,  %atom(), enabled | disabled
              plan,   %atom(), prepay | postpay
              services = []
            }
       ).
