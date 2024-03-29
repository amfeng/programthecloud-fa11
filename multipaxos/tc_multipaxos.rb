require 'rubygems'
require 'bud'
require 'test/unit'
require 'multipaxos'
require 'delivery/delivery'
require 'membership/membership'

### Test message sequences are are derived from
# http://en.wikipedia.org/wiki/Paxos_(computer_science)#Error_cases_in_basic_Paxos
#
# Note, the tests hang nondeterministically, so they should not be run as a group.
# Please run with
#   ruby tc_multipaxos.rb -n test_name
# to run each test individually.  The tests are:
#    test_basic_paxos
#    test_failure_learner
#    test_duelling_proposers
#    test_fail_before_accepting
#    test_fail_after_accepting
#    test_multi_paxos

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

  ### Simple test of basic paxos functionality.
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

   q = Queue.new
   p1.register_callback(:result) do |r|
    r.each do |row|
      assert_equal(row[1, 2], [:success, 'a'])
    end
    q.push true
   end

   p1.sync_do { p1.request <+ [[1, 'a']]}
   q.pop

   p1.stop
   a1.stop
   a2.stop
   a3.stop
   a4.stop
   a5.stop
  end

  ### This test attempts to emulate the "failure of redundant learner" case in
  ### http://en.wikipedia.org/wiki/Paxos_(computer_science)#Error_cases_in_basic_Paxos
  ### but hangs.  We are not sure if this is an actual bug or a problem with the
  ### test infrastructure that requres us to call p1.stop before we are able to
  ### register a callback for p1.result.
  def test_failure_learner
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
    a1.register_callback(:accepted_proposal) do
      # Stopping a1
      a1.stop
      p1.stop
      q.push true
    end

    q2 = Queue.new
    p1.register_callback(:result) do |r|
      r.each do |row|
        assert_equal(row[1, 2], [:success, 'a'])
      end
      q2.push true
    end

    p1.sync_do { p1.request <+ [[1, 'a']]}
    q.pop
    q2.pop

    p1.stop
    a1.stop
    a2.stop
    a3.stop
    a4.stop
    a5.stop
  end

  ### Tests what happens when propoers are unreliable.
  def test_duelling_proposers
    p1 = MultiPaxosTest.new(:port => 54320)
    p2 = MultiPaxosTest.new(:port => 54326)
    a1 = MultiPaxosTest.new(:port => 54321)
    a2 = MultiPaxosTest.new(:port => 54322)
    a3 = MultiPaxosTest.new(:port => 54323)
    a4 = MultiPaxosTest.new(:port => 54324)
    a5 = MultiPaxosTest.new(:port => 54325)
    p1.run_bg
    p2.run_bg
    a1.run_bg
    a2.run_bg
    a3.run_bg
    a4.run_bg
    a5.run_bg

    q2 = Queue.new
    p2.register_callback(:result) do |r|
      r.each do |row|
        assert_equal(row[1, 2], [:success, 'b'])
      end
      q2.push true
    end

    q1 = Queue.new
    a1.register_callback(:accepted_prepare) do
      p1.stop
      a1.stop
      p2.sync_do { p2.request <+ [[1, 'b']] }
    end
    q1.push true

    ### Warning, non-determinism.
    ### If you increase this request ID, then p2 needs to obtain a proposal
    ### number  that is higher than p1's proposal number in order to succeed.
    ### However, such a test hangs occasionally when p1 is initialized with a
    ### higher sequence number (e.g., 2).  We're not sure if it is a bug in the
    ### code or p2 is just taking a long time to increment its proposal number.
    ### Also, there is a rand(100000), so p1 could be bootstrapped with a very
    ### large initial proposal number compared to p2's proposal number.
    p1.sync_do { p1.request <+ [[1, 'a']]}
    q1.pop
    q2.pop

    p1.stop
    p2.stop
    a1.stop
    a2.stop
    a3.stop
    a4.stop
    a5.stop
  end

  # Test paxos for success after an acceptor fails before accepting a prepare request.
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
        assert_equal(row[1, 2], [:success, 'a'])
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

  # Test paxos for success after an acceptor fails after accepts a prepare request.
  def test_fail_after_accepting
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

    # Inserting a callback for a1's accepted_prepare
    # When the acceptor a1 accepts a request - stop this instance
    # Paxos should still succeed since there are 4 nodes still alive
    q1 = Queue.new
    a1.register_callback(:accepted_prepare) do
      # Stopping a1
      a1.stop
      q1.push true
    end

    q2 = Queue.new
    p1.register_callback(:result) do |r|
      r.each do |row|
        assert_equal(row[1, 2], [:success, 'a'])
      end
      q2.push true
    end

    p1.sync_do { p1.request <+ [[1, 'a']]}
    q1.pop
    q2.pop

    p1.stop
    a1.stop
    a2.stop
    a3.stop
    a4.stop
    a5.stop
  end

  ### Tests the multi in multipaxos.
  def test_multi_paxos
    p1 = MultiPaxosTest.new(:port => 54320)
    p2 = MultiPaxosTest.new(:port => 54326)
    a1 = MultiPaxosTest.new(:port => 54321)
    a2 = MultiPaxosTest.new(:port => 54322)
    a3 = MultiPaxosTest.new(:port => 54323)
    a4 = MultiPaxosTest.new(:port => 54324)
    a5 = MultiPaxosTest.new(:port => 54325)
    p1.run_bg
    p2.run_bg
    a1.run_bg
    a2.run_bg
    a3.run_bg
    a4.run_bg
    a5.run_bg

    q = Queue.new
    p2.register_callback(:result) do |r|
      r.each do |row|
        assert_equal(row[1, 2], [:success, 'b'])
      end
      q.push true
    end

    p1.sync_do { p1.request <+ [[1, 'a']]}
    p2.sync_do { p2.request <+ [[2, 'b']]}
    q.pop

    p1.stop
    p2.stop
    a1.stop
    a2.stop
    a3.stop
    a4.stop
    a5.stop
  end
end
