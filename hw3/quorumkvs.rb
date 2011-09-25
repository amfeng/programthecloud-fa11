require 'rubygems'
require 'bud'
require 'membership/membership'
require 'kvs/kvs'

module QuorumKVSProtocol
  state do
    interface input, :quorum_config, [] => [:r_fraction, :w_fraction]
    interface input, :kvput, [:client, :key] => [:reqid, :value]
    interface input, :kvdel, [:key] => [:reqid]
    interface input, :kvget, [:reqid] => [:key]
    interface output, :kvget_response, [:reqid] => [:key, :value]
  end
end

module QuorumKVS
  include QuorumKVSProtocol
  include StaticMembership
  import BasicKVS => :kvs

  state do
    table :config, [] => [:r_fraction, :w_fraction]
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

  bloom :actions do
  end
end

