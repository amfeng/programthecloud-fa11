require 'rubygems'
require 'bud'
require 'membership/membership'
require 'ordering/assigner'
require 'kvs/mv_kvs'

module QuorumKVSProtocol
  state do
    interface input, :quorum_config, [] => [:r_fraction, :w_fraction]
    interface input, :kvput, [:key] => [:reqid, :value]
    interface input, :kvdel, [:key] => [:reqid]
    interface input, :kvget, [:reqid] => [:key]
    interface output, :kvget_response, [:reqid] => [:key, :value]
    interface output, :kv_acks, [:reqid]
  end
end

module QuorumKVS
  include QuorumKVSProtocol
  include StaticMembership
  include SortAssign

  import BasicMVKVS => :mvkvs

  state do
    table :config, [] => [:r_fraction, :w_fraction]
    scratch :machinesToWrite, [:host] => [:ident]

    # Channels for sending requests to other machines
    channel :kvput_chan, [:@dest, :from] + kvput.key_cols => kvput.val_cols
    channel :kvdel_chan, [:@dest, :from] + kvdel.key_cols => kvdel.val_cols
    channel :kvget_chan, [:@dest, :from] + kvget.key_cols => kvget.val_cols
    channel :kvget_response_chan, [:@dest] + kvget_response.key_cols => kvget.val_cols
  end

  bloom :set_quorum_config do
    # Ignore quorum_config inputs if already set config inputs
    temp :adjusted_config <= quorum_config { |q|
      # Set R/W to 1 if input 0 is given
      [(q.r_fraction == 0 ? 1 : q.r_fraction),
       (q.w_fraction == 0 ? 1 : q.w_fraction)] 
    } 
    config <= adjusted_config.notin(config)
  end

  bloom :route do
    # Figure out how many machines need to write to, broadcast
    numberToWriteTo <= (member.length*config.w_fraction).ceil
    
    # If write, write to as many machines as needed
    # SortAggAssign assigns sequence numbers to items in the dump collection. 
    # Once we have sequence numbers we can pick items from the pickup collection with sequence number <= X.
    dump <= member
    machinesToWrite <= pickup {|machine| machine.payload if machine.ident <= numberToWriteTo}
    kvput_chan <= (machinesToWrite * kvput).pairs{|m, k| [m.host, ip_port] + k}

    # If read, set up a voting quorum for the necessary amount
    # of machines
    numberToReadFrom <= (member.length*config.r_fraction).ceil
    kvget_chan <= (member * kvget).pairs{|m,k| [m.host, ip_port] + k}
    
    # If del, write to as many machines as needed (?? do we 
    # need to delete from every machine?)
   end

  bloom :receive_requests do
    # If got a kv modification request, modify own table
    
    # FIXME: Not sure what to do about the client field. I think it's from??
    mvkvs.kvput <= kvput_chan { |k| [k.from, k.key, budtime, k.reqid, k.value]} 
    mvkvs.kvget <= kvget_chan { |k| [k.reqid, k.from, k.key, budtime]}

    # FIXME: MVKVS does not have a del - we need to add this!
    # FIXME: Do we actually need to implement del also??
    # mvkvs.kvdel <= kvdel_chan { |k| kvdel.schema.map { |c| k.send(c) }}

    # For get requests, send the response back to the original requestor
    kvget_response_chan <~ (kvget_chan*mvkvs.kvget_response).outer(:reqid => :reqid) { |c, r| [c.from] + r }
    kvget_response <= kvget_response_chan{|k| kvget_response.schema.map{|c| k.send(c)}} 
  end
end

