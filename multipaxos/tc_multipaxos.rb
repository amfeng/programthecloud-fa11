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

  # def test_basic_paxos
  #   p1 = MultiPaxosTest.new(:port => 54320)
  #   a1 = MultiPaxosTest.new(:port => 54321)
  #   a2 = MultiPaxosTest.new(:port => 54322)
  #   a3 = MultiPaxosTest.new(:port => 54323)
  #   a4 = MultiPaxosTest.new(:port => 54324)
  #   a5 = MultiPaxosTest.new(:port => 54325)
  #   p1.run_bg
  #   a1.run_bg
  #   a2.run_bg
  #   a3.run_bg
  #   a4.run_bg
  #   a5.run_bg

  #  q = Queue.new
  #  p1.register_callback(:result) do |r|
  #   r.each do |row|
  #     puts "Basic Multipaxos Output:", row.inspect
  #     assert_equal(row[1], :success)
  #   end
  #   q.push true
  #  end

  #  p1.sync_do { p1.request <+ [[1, 'a']]}
  #  q.pop

  #  p1.stop
  #  a1.stop
  #  a2.stop
  #  a3.stop
  #  a4.stop
  #  a5.stop
  # end

  # def test_failure_learner
  #   p1 = MultiPaxosTest.new(:port => 54320)
  #   a1 = MultiPaxosTest.new(:port => 54321)
  #   a2 = MultiPaxosTest.new(:port => 54322)
  #   a3 = MultiPaxosTest.new(:port => 54323)
  #   a4 = MultiPaxosTest.new(:port => 54324)
  #   a5 = MultiPaxosTest.new(:port => 54325)
  #   p1.run_bg
  #   a1.run_bg
  #   a2.run_bg
  #   a3.run_bg
  #   a4.run_bg
  #   a5.run_bg

  #   q = Queue.new
  #   a1.register_callback(:accepted_proposal) do
  #     puts "Entered a1's accepted_proposal callback"
  #     # Stopping a1
  #     p1.stop
  #     q.push true
  #   end
    
  #   q2 = Queue.new
  #   p1.register_callback(:result) do |r|
  #     r.each do |row|
  #       puts "Multipaxos Output:", row.inspect
  #       assert_equal(row[1], :success)
  #     end
  #     q2.push true
  #   end

  #   p1.sync_do { p1.request <+ [[1, 'a']]}
  #   q.pop
  #   q2.pop
    
  #   p1.stop
  #   a1.stop
  #   a2.stop
  #   a3.stop
  #   a4.stop
  #   a5.stop
  # end
  
  # def test_duelling_proposers
  #   p1 = MultiPaxosTest.new(:port => 54320)
  #   p2 = MultiPaxosTest.new(:port => 54326)
  #   a1 = MultiPaxosTest.new(:port => 54321)
  #   a2 = MultiPaxosTest.new(:port => 54322)
  #   a3 = MultiPaxosTest.new(:port => 54323)
  #   a4 = MultiPaxosTest.new(:port => 54324)
  #   a5 = MultiPaxosTest.new(:port => 54325)
  #   p1.run_bg
  #   p2.run_bg
  #   a1.run_bg
  #   a2.run_bg
  #   a3.run_bg
  #   a4.run_bg
  #   a5.run_bg

  #   q2 = Queue.new
  #   p2.register_callback(:result) do |r|
  #     r.each do |row|
  #       puts "Duelling Proposers Output:", row.inspect
  #       assert_equal(row[1], :success)
  #     end
  #     q2.push true
  #   end

  #   q1 = Queue.new
  #   a1.register_callback(:accepted_prepare) do
  #     puts "Entered a2's accepted_prepare callback"
  #     p1.stop
  #     a1.stop
  #     p2.sync_do { p2.request <+ [[1, 'a']] }
  #   end
  #   q1.push true

  #   p1.sync_do { p1.request <+ [[1, 'a']]}
  #   q1.pop
  #   q2.pop
    
  #   p1.stop
  #   p2.stop
  #   a1.stop
  #   a2.stop
  #   a3.stop
  #   a4.stop
  #   a5.stop
  # end
  
  # Test paxos for success after an acceptor fails before accepting a prepare request
  def test_fail_before_accepting
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
            
    q = Queue.new
    p1.register_callback(:result) do |r|
      r.each do |row|
        puts "Multipaxos Output:", row.inspect
        assert_equal(row[1], :success)
      end
      q.push true
    end
    
    p1.sync_do { 
      p1.request <+ [[1, 'a']]
      a1.stop
    }
    q.pop
    
    p1.stop
    a1.stop
    a2.stop
    a3.stop
    a4.stop
    a5.stop
  end

  # Test paxos for success after an acceptor fails after accepts a prepare request
  # def test_fail_after_accepting
  #   p1 = MultiPaxosTest.new(:port => 54320)
  #   a1 = MultiPaxosTest.new(:port => 54321)
  #   a2 = MultiPaxosTest.new(:port => 54322)
  #   a3 = MultiPaxosTest.new(:port => 54323)
  #   a4 = MultiPaxosTest.new(:port => 54324)
  #   a5 = MultiPaxosTest.new(:port => 54325)
  #   p1.run_bg
  #   a1.run_bg
  #   a2.run_bg
  #   a3.run_bg
  #   a4.run_bg
  #   a5.run_bg
    
  #   # Inserting a callback for a1's accepted_prepare
  #   # When the acceptor a1 accepts a request - stop this instance
  #   # Paxos should still succeed since there are 4 nodes still alive
  #   q1 = Queue.new
  #   a1.register_callback(:accepted_prepare) do
  #     puts "Entered a1's accepted_prepare callback"
  #     # Stopping a1
  #     a1.stop
  #     q1.push true
  #   end
    
  #   # q2 = Queue.new
  #   # a2.register_callback(:accepted_prepare) do
  #   #   puts "Entered a2's accepted_prepare callback"
  #   #   # Stopping a1
  #   #   a2.stop
  #   #   q2.push true
  #   # end
    
  #   q3 = Queue.new
  #   p1.register_callback(:result) do |r|
  #     r.each do |row|
  #       puts "Multipaxos Output:", row.inspect
  #       assert_equal(row[1], :success)
  #     end
  #     q3.push true
  #   end
    
  #   p1.sync_do { p1.request <+ [[1, 'a']]}
  #   q1.pop
  #   # q2.pop
  #   q3.pop
    
  #   # p1.register_callback(:result) do |r|
  #   #   assert_equal(1,1)
  #   #   r.each do |row|
  #   #     puts "Multipaxos Output:", row.inspect
  #   #     assert_equal(row[1], :success)
  #   #   end

  #   #   puts "Requests:", p1.result.first.inspect
  #   #   puts "Acceptor 1:", a1.accepted_prepare.first.inspect
  #   #   puts "Acceptor 2:", a2.accepted_prepare.first.inspect
  #   #   puts "Acceptor 3:", a3.accepted_prepare.first.inspect
  #   #   puts "Acceptor 4:", a4.accepted_prepare.first.inspect
  #   #   puts "Acceptor 5:", a5.accepted_prepare.first.inspect
  #   # end

  #   # # p1.sync_callback(:request, [[1, 'a']], :result)
  #   # p1.sync_do{ p1.request <+ [[1, 'a']]}
  #   # # p1.tick
  #   # # p1.tick
  #   # # p1.tick
  #   # # p1.tick
  #   # # p1.tick
  #   # # p1.tick

  #   # sleep 5

  #   # a2.register_callback(:accepted_prepare) do
  #   #   assert_equal(1,1)
  #   # end

  #   # a3.register_callback(:accepted_prepare) do
  #   #   assert_equal(1,1)
  #   # end

  #   # a1.stop


  #   p1.stop
  #   a1.stop
  #   a2.stop
  #   a3.stop
  #   a4.stop
  #   a5.stop
  # end

  # def test_multi_paxos
  #   p1 = MultiPaxosTest.new(:port => 54320)
  #   p2 = MultiPaxosTest.new(:port => 54326)
  #   a1 = MultiPaxosTest.new(:port => 54321)
  #   a2 = MultiPaxosTest.new(:port => 54322)
  #   a3 = MultiPaxosTest.new(:port => 54323)
  #   a4 = MultiPaxosTest.new(:port => 54324)
  #   a5 = MultiPaxosTest.new(:port => 54325)
  #   p1.run_bg
  #   p2.run_bg
  #   a1.run_bg
  #   a2.run_bg
  #   a3.run_bg
  #   a4.run_bg
  #   a5.run_bg

  #   q = Queue.new
  #   p2.register_callback(:result) do |r|
  #     r.each do |row|
  #       puts "Multipaxos Output:", row.inspect
  #       assert_equal(row[1], :success)
  #     end
  #     q.push true
  #   end

  #   p1.sync_do { p1.request <+ [[1, 'a']]}
  #   p2.sync_do { p2.request <+ [[2, 'b']]}
  #   q.pop
    
  #   p1.stop
  #   p2.stop
  #   a1.stop
  #   a2.stop
  #   a3.stop
  #   a4.stop
  #   a5.stop
  # end
end
