require 'rubygems'
require 'bud'
require 'kvs/mv_kvs'

module VersionMVKVS
  include BasicMVKVS

  state do
    scratch :matching, [:reqid, :key, :version, :value]
    scratch :max_version, [:key] => [:version]
  end

  bloom :get do
    # Since MVKVS doesn't overwrite on key updates (it keeps a separate
    # version for each update), we need to find the one with the highest
    # version number and return that.

    # Find matching key-val pairs
    matching <= (kvget * kvstate).pairs(:key => :key) { |g, s|
      [g.reqid, s.key, s.version, s.value]
    }

    stdio <~ matching.inspected

    # Including only the largest version for the key requested
    max_version <= matching.group([:key], max(:version))
    kvget_response <= (matching * max_version).lefts(:key => :key, :version => :version)
  end
end
