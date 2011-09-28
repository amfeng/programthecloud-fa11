require 'rubygems'
require 'bud'
require 'kvs/mv_kvs'

module MVKVSD
  include BasicMVKVS

  state do
    interface output, :kvput_response, [:reqid, :key, :version]
    interface output, :kvdel_response, [:reqid, :key, :version]
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
