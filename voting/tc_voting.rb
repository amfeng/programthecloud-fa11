require 'rubygems'
require 'bud'
require 'test/unit'
require 'voting'

class TestVoting < Test::Unit::TestCase
  class RatioVotingBloom
    include Bud
    include RatioVoteCounter
  end

  class UnanimousVotingBloom
    include Bud
    include UnanimousVoteCounter
  end

  class MajorityVotingBloom
    include Bud
    include MajorityVoteCounter
  end

  def basic_checks(expected_id, expected_status, expected_response, resps)
    assert_equal(expected_id, resps.first[0])
    assert_equal(expected_status, resps.first[1])
    assert_equal(expected_response, resps.first[2])
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
    basic_checks(1, :success, 'Obama', resps)
    # Additional assertions
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
    basic_checks(2, :success, 'Obama', resps)
    
    p1.stop
  end

  def test_ratio_voter_fail
    p1 = RatioVotingBloom.new
    p1.run_bg
    p2 = RatioVotingBloom.new
    p2.run_bg
    
    # Test failure given a ratio of 1
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.ratio <+ [[1, 1]]}
    p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, 'McCain', 'second']], 
                             p1.result.tabname)
    basic_checks(1, :fail, nil, resps)

    # Test failure given a ratio of 0.5
    p2.sync_do {p2.begin_vote <+ [[2, 3]]}
    p2.sync_do {p2.ratio <+ [[2, 0.5]]} # Will need 2 of the 3 votes
    p2.sync_do {p2.cast_vote <+ [[2, 'Obama', 'first']]}
    p2.sync_do {p2.cast_vote <+ [[2, 'McCain', 'second']]}
    resps = p2.sync_callback(p2.cast_vote.tabname, [[2, 'Nader', 'third']], 
                             p2.result.tabname)
    basic_checks(2, :fail, nil, resps)

    p1.stop
    p2.stop
  end

  def test_unanimous_voter
    p1 = UnanimousVotingBloom.new
    p1.run_bg
    p2 = UnanimousVotingBloom.new
    p2.run_bg

    # Test success
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, 'Obama', 'second']], 
                             p1.result.tabname)
    basic_checks(1, :success, 'Obama', resps)
    # Additional checks
    assert_equal(['Obama', 'Obama'], resps.first[3])
    assert_equal(true, resps.first[4].include?('second'))
    assert_equal(true, resps.first[4].include?('first'))

    # Test fail
    p2.sync_do {p2.begin_vote <+ [[2, 2]]}
    p2.sync_do {p2.cast_vote <+ [[2, 'Obama', 'first']]}
    resps = p2.sync_callback(p2.cast_vote.tabname, [[2, 'McCain', 'second']], 
                             p2.result.tabname)
    basic_checks(2, :fail, nil, resps)
    
    p1.stop
    p2.stop
  end

  def test_majority_voter
    p1 = MajorityVotingBloom.new
    p1.run_bg
    p2 = MajorityVotingBloom.new
    p2.run_bg
    p3 = MajorityVotingBloom.new
    p3.run_bg

    # Test success in a 2 agent case (need both)
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, 'Obama', 'second']], 
                             p1.result.tabname)
    basic_checks(1, :success, 'Obama', resps)
    # Additional checks
    assert_equal(['Obama', 'Obama'], resps.first[3])
    assert_equal(true, resps.first[4].include?('second'))
    assert_equal(true, resps.first[4].include?('first'))

    # Test success in a 4 agent case (need 3)
    p2.sync_do {p2.begin_vote <+ [[2, 4]]}
    p2.sync_do {p2.cast_vote <+ [[2, 'Obama', 'first']]}
    p2.sync_do {p2.cast_vote <+ [[2, 'Obama', 'second']]}
    p2.sync_do {p2.cast_vote <+ [[2, 'McCain', 'third']]}
    resps = p2.sync_callback(p2.cast_vote.tabname, [[2, 'Obama', 'fourth']], 
                             p2.result.tabname)
    basic_checks(2, :success, 'Obama', resps)
    # Additional checks
    assert_equal(3, resps.first[3].count('Obama'))
    assert_equal(1, resps.first[3].count('McCain'))

    # Test fail in a 2 agent case (didn't get 2)
    p3.sync_do {p3.begin_vote <+ [[3, 2]]}
    p3.sync_do {p3.cast_vote <+ [[3, 'Obama', 'first']]}
    resps = p3.sync_callback(p3.cast_vote.tabname, [[3, 'McCain', 'second']], 
                             p3.result.tabname)
    basic_checks(3, :fail, nil, resps)
    
    p1.stop
    p2.stop
    p3.stop
  end

end
