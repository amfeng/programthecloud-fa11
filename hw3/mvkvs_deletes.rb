require 'rubygems'
require 'bud'
require 'kvs/mv_kvs'

module MVKVSD
  include BasicMVKVS

  state do
    interface input, :kvdel, [:client, :key, :version] => [:reqid]
    interface output, :kvput_response, [:reqid, :key, :version]
    interface output, :kvdel_response, [:reqid, :key, :version]
    interface output, :kvget_response_ip, [:ip, :reqid, :key, :version] => [:value]
    scratch :matching, [:reqid, :key, :version, :value]
    scratch :max_version, [:key] => [:version]
    
  end

  bloom do
    kvget_response_ip <= kvget_response {|k| [ip_port] + k}
  end

  bloom :get do
    matching <= (kvget * kvstate).pairs(:key => :key) { |g, s|
      [g.reqid, s.key, s.version, s.value]
    }

    # Including only the largest version for the key requested
    #kvget_response <= (kvget * kvstate * matching.group([:key], max[:version])).combos(kvget.key => matching.key, kvstate.key => matching.key, kvstate.version => matching.version) { do |g, s, m| [g.reqid, s.key, s.version, s.value] }
    max_version <= matching.group([:key], max(:version))
    kvget_response <= (matching * max_version).lefts(:key => :key, :version => :version)
  end

  bloom :put do
    kvstate <= kvput {|s| [s.key, s.version, s.value]}
    kvput_response <= kvput {|s| [s.reqid, s.key, s.version]}
  end

  bloom :del do
    # A delete is just a put with value nil
    kvput <= kvdel {|s| s + [nil]}
    kvdel_response <= kvdel {|s| [s.reqid, s.key, s.version]}
  end
end
