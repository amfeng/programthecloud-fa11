require 'rubygems'
require 'bud'
require 'test/unit'
require 'multipaxos'
require 'delivery/delivery'
require 'membership/membership'

class TestVoting < Test::Unit::TestCase
  class MultiPaxosTest
    include Bud
    include PaxosProtocol
    include PaxosInternalProtocol
    include Paxos
    include BestEffortDelivery
    include StaticMembership
    
    bootstrap do
      add_member <= [['localhost:54321', 1],
                     ['localhost:54322', 2],
                     ['localhost:54323', 3],
                     ['localhost:54324', 4],
                     ['localhost:54325', 5]]
    end
  end

  # Test for basic paxos version
  def test_basic_paxos
    p1 = MultiPaxosTest.new(:port => 54320)
    a1 = MultiPaxosTest.new(:port => 54321)
    a2 = MultiPaxosTest.new(:port => 54322)
    a3 = MultiPaxosTest.new(:port => 54323)
    a4 = MultiPaxosTest.new(:port => 54324)
    a5 = MultiPaxosTest.new(:port => 54325)
    p1.run_bg
    a1.run_bg
    a2.run_bg
    a3.run_bg
    a4.run_bg
    a5.run_bg

    p1.sync_callback(:request, [[1, 'a']], :result)
    p1.register_callback(:result) do |r|
      r.each do |row|
        puts "Multipaxos Output:", row.inspect
        assert_equal(row[1], :success)
      end
    end

    p1.stop
    a1.stop
    a2.stop
    a3.stop
    a4.stop
    a5.stop
  end
end
