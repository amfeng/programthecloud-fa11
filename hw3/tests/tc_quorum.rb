require 'rubygems'
require 'bud'
require 'test/unit'
require 'kvs/quorum_kvs'

class TestQuorum < Test::Unit::TestCase
  class MajorityBloom
    include Bud
    include QuorumKVS

    bootstrap do
      quorum_config <= [[0.51, 0.51]]
      add_member <= [['localhost:54321', 1],['localhost:54322', 2],['localhost:54323', 3],['localhost:54324', 4],['localhost:54325', 5]]
    end
  end
  
  class ROWABloom
    include Bud
    include QuorumKVS

    bootstrap do
      quorum_config <= [[0, 1.0]]
      add_member <= [['localhost:54321', 1],['localhost:54322', 2],['localhost:54323', 3],['localhost:54324', 4],['localhost:54325', 5]]
    end
  end
  
  class ROWOBloom
    include Bud
    include QuorumKVS

    bootstrap do
      quorum_config <= [[0, 0]]
      add_member <= [['localhost:54321', 1],['localhost:54322', 2],['localhost:54323', 3],['localhost:54324', 4],['localhost:54325', 5]]
    end
  end

  def start_servers(n, klass)
    p = []
    (1..n).each do |i|
      p[i] = klass.new(:port=>(54320+i))
      p[i].run_bg
    end
    p
  end
  
  def stop_servers(p)
    (1..p.length-1).each do |i| 
      p[i].stop
    end
  end
  
  def simple_on_3(p)
    acks = p[1].sync_do {p[1].kvput <+ [[1, "joe", 1, "hellerstein"]]}
    acks = p[2].sync_do {p[2].kvput <+ [[2, "peter", 2, "alvaro"]]}
    acks = p[3].sync_do {p[3].kvput <+ [[3, "joe", 3, "piscopo"]]}
    acks = p[3].sync_do {p[3].kvput <+ [[3, "peter", 4, "tosh"]]}
    resps = p[1].sync_callback(p[1].kvget.tabname, [[5, "joe"]], p[1].kvget_response.tabname)
    assert_equal([[5, "joe", "piscopo"]], resps)
    resps = p[3].sync_callback(p[1].kvget.tabname, [[6, "joe"]], p[1].kvget_response.tabname)
    assert_equal([[6, "joe", "piscopo"]], resps)
    resps = p[1].sync_callback(p[1].kvget.tabname, [[7, "peter"]], p[1].kvget_response.tabname)
    assert_equal([[7, "peter", "tosh"]], resps)
    resps = p[3].sync_callback(p[3].kvget.tabname, [[8, "peter"]], p[1].kvget_response.tabname)
    assert_equal([[8, "peter", "tosh"]], resps)
  end

  def test_simple_majority
    p = start_servers(5, MajorityBloom)
    simple_on_3(p)
    stop_servers(p)
  end
  
  def test_simple_rowa
    p = start_servers(5, ROWABloom)
    simple_on_3(p)
    stop_servers(p)
  end
  def test_simple_rowo
    p = start_servers(5, ROWOBloom)
    simple_on_3(p)
    stop_servers(p)
  end
end

