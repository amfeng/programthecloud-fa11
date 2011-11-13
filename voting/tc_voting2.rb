require 'rubygems'
require 'bud'
require 'test/unit'
require 'voting2'

class TestVoting < Test::Unit::TestCase
  class RatioVotingBloom
    include Bud
    include RatioVoteCounter
  end

  class UnanimousVotingBloom
    include Bud
    include UnanimousVoteCounter
  end

  def test_ratio_voter_success
    p1 = RatioVotingBloom.new
    p1.run_bg

    # Test success given a ratio of 1
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.ratio <+ [[1, 1]]}
    p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'first']]}
    #p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'second']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, 'Obama', 'second']], 
                             p1.result.tabname)
    assert_equal(1, resps.first[0])
    assert_equal(:success, resps.first[1])
    assert_equal('Obama', resps.first[2])
    assert_equal(['Obama', 'Obama'], resps.first[3])
    assert_equal(true, resps.first[4].include?('second'))
    assert_equal(true, resps.first[4].include?('first'))

    # Test success given a ratio of 0.5
    p1.sync_do {p1.begin_vote <+ [[2, 3]]}
    p1.sync_do {p1.ratio <+ [[2, 0.5]]} # Will need 2 of the 3 votes
    p1.sync_do {p1.cast_vote <+ [[2, 'Obama', 'first']]}
    p1.sync_do {p1.cast_vote <+ [[2, 'McCain', 'second']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[2, 'Obama', 'third']], 
                             p1.result.tabname)
    assert_equal(2, resps.first[0])
    assert_equal(:success, resps.first[1])
    
    p1.stop
  end

  def test_ratio_voter_fail
    p1 = RatioVotingBloom.new
    p1.run_bg
    
    # Test failure given a ratio of 1
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.ratio <+ [[1, 1]]}
    p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, 'McCain', 'second']], 
                             p1.result.tabname)
    assert_equal(1, resps.first[0])
    assert_equal(:fail, resps.first[1])

    # Test failure given a ratio of 0.5
    p1.sync_do {p1.begin_vote <+ [[2, 3]]}
    p1.sync_do {p1.ratio <+ [[2, 0.5]]} # Will need 2 of the 3 votes
    p1.sync_do {p1.cast_vote <+ [[2, 'Obama', 'first']]}
    p1.sync_do {p1.cast_vote <+ [[2, 'McCain', 'second']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[2, 'Nader', 'third']], 
                             p1.result.tabname)
    assert_equal(2, resps.first[0])
    assert_equal(:fail, resps.first[1])

    p1.stop
  end

  def test_unanimous_voter
    p1 = UnanimousVotingBloom.new
    p1.run_bg

    # Test success
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, 'Obama', 'second']], 
                             p1.result.tabname)
    assert_equal(1, resps.first[0])
    assert_equal(:success, resps.first[1])
    assert_equal('Obama', resps.first[2])
    assert_equal(['Obama', 'Obama'], resps.first[3])
    assert_equal(true, resps.first[4].include?('second'))
    assert_equal(true, resps.first[4].include?('first'))

    # Test fail
    p1.sync_do {p1.begin_vote <+ [[2, 2]]}
    p1.sync_do {p1.cast_vote <+ [[2, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[2, 'McCain', 'second']], 
                             p1.result.tabname)
    
    assert_equal(2, resps.first[0])
    assert_equal(:fail, resps.first[1])
    
    p1.stop
  end
  
end
