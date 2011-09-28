require 'rubygems'
require 'bud'
require 'membership/membership'
require 'ordering/assigner'
require '../mvkvs_deletes'
require '../vote_counting'

module QuorumKVSProtocol
  state do
    interface input, :quorum_config, [] => [:r_fraction, :w_fraction]
    interface input, :kvput, [:client, :key] => [:reqid, :value]
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

  import MVKVSD => :mvkvs
  import VoteCounting => :voting

  state do
    table :config, [] => [:r_fraction, :w_fraction]
    table :getResponsesReceived, [:reqid] => [:key, :value]
    scratch :machinesToWrite, [:host] => [:ident]
    table :numberToWriteTo, [:num]
    table :numberToReadFrom, [:num]

    # Channels for sending requests to other machines
    channel :kvput_chan, [:@dest, :from] + kvput.key_cols => kvput.val_cols
    channel :kvput_response_chan, [:@dest] + mvkvs.kvput_response.key_cols => mvkvs.kvput.val_cols

    channel :kvdel_chan, [:@dest, :from] + kvdel.key_cols => kvdel.val_cols
    channel :kvdel_response_chan, [:@dest] + mvkvs.kvdel_response.key_cols => mvkvs.kvdel.val_cols

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
    config <+ adjusted_config.notin(config)

    # Since these numbers will never change, set them now
    # FIXME: Change to the actual numbers, not percentages
    numberToWriteTo <= quorum_config {|q| [member.length * q.w_fraction] }
    numberToReadFrom <= quorum_config {|q| [member.length * q.r_fraction] }
  end

  bloom :route do
    # Figure out how many machines need to write to, broadcast
    
    # If write, write to as many machines as needed
    # SortAggAssign assigns sequence numbers to items in the dump collection. 
    # Once we have sequence numbers we can pick items from 
    # the pickup collection with sequence number <= X.

    # dump <= member
    # machinesToWrite <= pickup {|machine| machine.payload if machine.ident <= numberToWriteTo}
    # kvput_chan <~ (machinesToWrite * kvput).pairs{|m, k| [m.host, ip_port] + k}

    
    # If read, set up a voting quorum for the necessary amount
    # of machines
    # FIXME: Figure out how to calculate numberToReadFrom and get that many machines
    #        and send a read request to only them
    # numberToReadFrom <= [[(member.length * config.r_fraction).ceil]]

    # voting.numberRequired <= numberToReadFrom

    # If del, write to W many machines
    voting.numberRequired <= [[member.length]]

    kvget_chan <~ (member * kvget).pairs{|m,k| [m.host, ip_port] + k}
    kvput_chan <~ (member * kvput).pairs{|m, k| [m.host, ip_port] + k}
    kvdel_chan <~ (member * kvdel).pairs{|m,k| [m.host, ip_port] + k}
    
    voting.incomingRows <= kvget_response_chan
    voting.incomingRows <= kvput_response_chan
    voting.incomingRows <= kvdel_response_chan
    
    # What do we do about puts and deletes?
    # Maybe return all the reqid's that have been successfully acked - so we can put that in our output interface?
    kvget_response <= voting.result
   end

  bloom :receive_requests do
    # If got a kv modification request, modify own table
    
    # FIXME: Not sure what to do about the client field. I think it's from??
    mvkvs.kvput <= kvput_chan { |k| [k.from, k.key, budtime, k.reqid, k.value]} 
    kv_acks <= kvput_chan { |k| [k.reqid] }

    mvkvs.kvget <= kvget_chan { |k| [k.reqid, k.from, k.key, budtime]}
    mvkvs.kvdel <= kvdel_chan { |k| [k.from, k.key, budtime, k.reqid]}
    
    # FIXME: MVKVS does not have a del - we need to add this!
    # mvkvs.kvdel <= kvdel_chan { |k| kvdel.schema.map { |c| k.send(c) }}

    # For get requests, send the response back to the original requestor
    kvget_response_chan <~ (kvget_chan * mvkvs.kvget_response).outer(:reqid => :reqid) { |c, r| [c.from] + r }
    kvput_response_chan <~ (kvput_chan * mvkvs.kvput_response).outer(:reqid => :reqid) { |c, r| [c.from] + r}
    kvdel_response_chan <~ (kvdel_chan * mvkvs.kvdel_response).outer(:reqid => :reqid) { |c, r| [c.from] + r}
    # If so, find the value for that key that has the largest budtime and put that into kvget_response
    # incomingRows <= kvget_response_chan {|k| kvget_response.schema.map {|c| k.send(c)}}
    
    # kvget_response <= kvget_response_chan{|k| kvget_response.schema.map{|c| k.send(c)}} 
  end
end

