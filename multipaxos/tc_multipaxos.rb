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

  ###
  ### Test for basic paxos version
  ###
  def test_paxos
    p1 = MultiPaxosTest.new
    p1.run_bg

    p1.sync_callback(:request, [[1, 'a']], :result)
    assert_equal([[1, :accept]], result)

    # p1.sync_do {p1.request <+ [['a', 1]]}
    # p1.register_callback(:result) do |r|
    #   r.each do |row|
    #     puts "ROW ASSERTION"
    #     assert_equal(row.ident, 'a')
    #     puts row.status
    #   end
    # end

    #p1.sync_do {p1.begin_vote <+ [[1, 2]]; p1.ratio <+ [[1, 1]]}
    #p1.sync_do {p1.cast_vote <+ [[1, :A, 'Obama', 'first']]}

    

    #resps = p1.sync_callback(p1.cast_vote.tabname, [[1, :B, 'Obama', 'second']], p1.result.tabname)
    #basic_checks(1, :success, 'Obama', resps)

    # Check accumulated votes/notes
    #assert_equal(['Obama', 'Obama'], resps.first[3])
    #assert_equal(true, resps.first[4].include?('second'))
    #assert_equal(true, resps.first[4].include?('first'))

    # Test success given a ratio of 0.5
    #p1.sync_do {p1.begin_vote <+ [[2, 3]]; p1.ratio <+ [[2, 0.5]]}
    #p1.sync_do {p1.cast_vote <+ [[2, :A, 'Obama']]}
    #p1.sync_do {p1.cast_vote <+ [[2, :B, 'McCain']]}
    #resps = p1.sync_callback(p1.cast_vote.tabname, [[2, :C, 'Obama']], 
    #                         p1.result.tabname)
    #basic_checks(2, :success, 'Obama', resps)    
    #p1.stop
  end
end
