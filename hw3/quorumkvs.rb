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
    interface input, :kvget, [:reqid] => [:key]

    interface output, :kvget_response, [:reqid] => [:key, :value]
    interface output, :kv_acks, [:reqid]
  end
end

module QuorumKVS
  include QuorumKVSProtocol
  include StaticMembership

  import MVKVSD => :mvkvs
  import VoteCounting => :voting

  state do
    # Config, number to R/W from
    table :config, [] => [:r_fraction, :w_fraction]
    table :numberToWriteTo, [:num]
    table :numberToReadFrom, [:num]

    # Channels for sending requests to other machines
    channel :kvput_chan, [:@dest, :from] + kvput.key_cols => kvput.val_cols
    channel :kvget_chan, [:@dest, :from] + kvget.key_cols => kvget.val_cols

    # Channel to send response back to requesting machine
    channel :kvget_response_chan, [:@dest, :from, :reqid, :key, :version] => [:value] 
    channel :kvput_response_chan, [:@dest, :from, :reqid, :key]

    # Tables to waiting get/put requests in while we wait for acks
    table :kvget_queue, [:dest, :from, :reqid]  => [:key]
    table :kvput_queue, [:dest, :from, :reqid]  => [:key, :value]

    # TODO: Encapsulate versioning in the KVS itself 
    table :current_version, [] => [:version]

    # FIXME: Remove this table, use current_version instead
    # This is a table to keep track of the count of writes - used for versioning
    # table :currentCount, [] => [:count]
    table :processedReqid, [:reqid]
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

    # FIXME: Change to actual number read/write required
    voting.numberRequired <= [[member.length]]
  end

  bloom :route do
    # Broadcast to all, then return when have a sufficient number of acks
    kvget_chan <~ (member * kvget).pairs{|m,k| [m.host, ip_port] + k}
    kvput_chan <~ (member * kvput).pairs{|m, k| [m.host, ip_port] + k}
    
    # Send get responses to vote counter for counting
    voting.incomingRows <= kvget_response_chan { |r|
      [r.from, r.reqid, r.key, r.version, r.value]
    }

    # TODO: Send put responses to vote counter for counting
    # voting.incomingRows <= kvput_response_chan

    # Vote counter sends back the result once we've gotten a sufficient
    # number of acks
    # TODO: Generalize this for both types (read, writes)
    kvget_response <= voting.result
   end

  # FIXME: Redo; set current_version instead
  bloom :versioning do
    current_version <+- [[budtime]]
    #temp :unprocessedPuts <= kvput.notin(processedReqid, :reqid => :reqid)
    #processedReqid <+ unprocessedPuts {|t| [t.reqid]} 
    # currentCount <+- currentCount {|c| [c.count + unprocessedPuts.length]}

    #temp :unprocessedDels <= kvdel.notin(processedReqid, :reqid => :reqid)
    #processedReqid <+ unprocessedDels  {|t| [t.reqid]}
    # currentCount <+- currentCount {|c| [c.count + unprocessedDels.length]}
  end

  bloom :receive_requests do
    # If got a kv modification request, modify own table
    kvget_queue <= kvget_chan
    
    mvkvs.kvput <= (current_version * kvput_chan).pairs { |v, k| [k.client, k.key, v.version, k.reqid, k.value]} 

    # FIXME: Count number of put acks we got back, right now, blindly sending bakc
    # ack
    kv_acks <= kvput_chan { |k| [k.reqid] }

    mvkvs.kvget <= (current_version * kvget_chan).pairs { |v, k| [k.reqid, k.from, k.key, v.version]}
    
    # For get requests, send the response back to the original requestor
    kvget_response_chan <~ (kvget_queue * mvkvs.kvget_response).pairs(:reqid => :reqid) { |c, r| [c.from, c.dest] + r }
  end
end

