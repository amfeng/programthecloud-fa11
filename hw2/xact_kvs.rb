require 'rubygems'
require 'bud'
require 'kvs/kvs'
require '../lckmgr'

module XactKVSProtocol
  include BasicKVS
  state do
    interface input, :xput, [:xid, :key, :reqid] => [:data]
    interface input, :xget, [:xid, :key, :reqid]
    interface output, :xget_response, [:xid, :key, :reqid] => [:data]
    interface output, :xput_response, [:xid, :key, :reqid]
  end
end

module TwoPLTransactionalKVS
  include XactKVSProtocol
  include TwoPhaseLockMgr

  state do
    table :put_queue, [:xid, :key, :reqid] => [:data]
  end

  # Perform the puts
  bloom :mutate do
    # Request a :X lock before performing the put
    request_lock <= xput {|x| [x.xid, x.key, :X]}

    # Remember that we wanted to do a put
    put_queue <= xput
    
    # Once we have obtained the lock, send that put request to basickvs
    kvput <= (put_queue * lock_status).lefts(:xid => :xid, :key => :resource) 

    # Update xput_response to indicate that we are done
    xput_response <= (put_queue * lock_status).lefts(:xid => :xid, :key => :resource) {|put| [put.xid, put.key, put.reqid]}
    
    # Remove the put request from put_queue
    put_queue <- (put_queue * xput_response).lefts(:xid => :xid, :key => :key, :reqid => :reqid)
  end

  # Perform the gets
  bloom :get do
    request_lock <+ xget {|x| [x.xid, x.key, :S]}
    kvget <+ (xget * lock_status).lefts(:xid => :xid, :key => :resource)
    xget_response <+ (kvget_response * xget).pairs(:key => :key) {|resp, get| [get.xid, resp.key, resp.reqid, resp.value]}
  end

  bloom :debug do
    stdio <~ [["tick #{budtime}"]]
    stdio <~ xput.inspected
    stdio <~ xget.inspected
    stdio <~ request_lock.inspected
    stdio <~ locks.inspected
    stdio <~ lock_status
  end
end
