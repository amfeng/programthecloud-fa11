require 'rubygems'
require 'bud'
require 'test/unit'
require 'multipaxos'
require 'delivery/delivery'
require 'membership/membership'

class TestVoting < Test::Unit::TestCase
  class MultiPaxosTest
    include Bud
    #include PaxosProtocol
    #include PaxosInternalProtocol
    include Paxos
    # include BestEffortDelivery
    # include StaticMembership
    
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
    p1 = MultiPaxosTest.new
    p1.run_bg

    p1.sync_callback(:request, [[1, 'a']], :result)
    assert_equal([[1, :accept]], result)
  end

  # Test for multi paxos version
  def test_multi_paxos
    p1 = MultiPaxosTest.new
    p1.run_bg

    p1.sync_do {p1.request <+ [[1, 'a']]}
    p1.sync_do {p1.request <+ [[2, 'a']]}
    p1.sync_do {p1.request <+ [[3, 'a']]}
    p1.register_callback(:accept) do |r|
      r.each do |row|        
        assert_equal([[3, :accept]], result)    
      end
    end
  end

  # Test for multi paxos version
  def test_multi_paxos2
    p1 = MultiPaxosTest.new(:port=>54320)
    p1.run_bg
    p2 = MultiPaxosTest.new(:port=>54321)
    p2.run_bg
    p3 = MultiPaxosTest.new(:port=>54322)
    p3.run_bg
    p4 = MultiPaxosTest.new(:port=>54323)
    p4.run_bg
    p5 = MultiPaxosTest.new(:port=>54324)
    p5.run_bg

    p1.sync_do {p1.request <+ [[1, 'a']]}
    p1.sync_do {p1.request <+ [[2, 'a']]}
    p1.sync_do {p1.request <+ [[3, 'a']]}
    p1.register_callback(:accept) do |r|
      r.each do |row|        
        assert_equal([[3, :accept]], result)    
      end
    end
  end
    # p1.sync_do {p1.request <+ [['a', 1]]}
    # p1.register_callback(:result) do |r|
    #   r.each do |row|
    #     puts "ROW ASSERTION"
    #     assert_equal(row.ident, 'a')
    #     puts row.status
    #   end
    # end
end
