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
    table :get_queue, [:xid, :key, :reqid]

    # Intermediate tables for grouping and joining to allow only
    # one put at each timestep
    scratch :can_put, [:xid, :key, :reqid] => [:data]
    scratch :to_put, [:xid, :key, :reqid] => [:data]
  end

  # Perform the puts
  bloom :pl_mutate do
    # Request a :X lock before performing the put
    request_lock <= xput {|x| [x.xid, x.key, :X]}

    # Remember that we wanted to do a put
    put_queue <= xput
    
    # Once we have obtained the lock, send that put request to basickvs
    can_put <= (put_queue * lock_status).lefts(:xid => :xid, :key => :resource) 
    to_put <= can_put.argmin([:xid, :key], :reqid)
    kvput <= (to_put * can_put).rights(:xid => :xid, :key => :key)

    # Update xput_response to indicate that we are done
    xput_response <= (put_queue * lock_status).lefts(:xid => :xid, :key => :resource) {|put| [put.xid, put.key, put.reqid]}
    
    # Remove the put request from put_queue
    put_queue <- (put_queue * xput_response).lefts(:xid => :xid, :key => :key, :reqid => :reqid)
  end

  # Perform the gets
  bloom :pl_get do
    # Request a :S lock before the get
    request_lock <= xget {|x| [x.xid, x.key, :S]}
    
    # Remeber that we wanted to do a get
    get_queue <= xget

    # Once we have obtained the lock, send the get request to basickvs
    kvget <= (get_queue * lock_status).lefts(:xid => :xid, :key => :resource) {|get| [get.reqid, get.key]}
    
    # Update xget_response to indicate that we are done
    xget_response <= (kvget_response * get_queue).pairs(:reqid => :reqid) {|resp, get| [get.xid, resp.key, resp.reqid, resp.value]}

    # Remove the get request from get_queue
    get_queue <- (get_queue * xget_response).lefts(:xid => :xid, :key => :key, :reqid => :reqid)
  end
end
