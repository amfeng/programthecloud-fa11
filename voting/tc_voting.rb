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

  def test_count_voter
    # TODO
  end

  def test_ratio_voter_success
    p1 = RatioVotingBloom.new
    p1.run_bg

    # Test success given a ratio of 1
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.ratio <+ [[1, 1]]}
    p1.sync_do {p1.cast_vote <+ [[1, :A, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, :B, 'Obama', 'second']], 
                             p1.result.tabname)
    basic_checks(1, :success, 'Obama', resps)

    # Check accumulated votes/notes
    assert_equal(['Obama', 'Obama'], resps.first[3])
    assert_equal(true, resps.first[4].include?('second'))
    assert_equal(true, resps.first[4].include?('first'))

    # Test success given a ratio of 0.5
    p1.sync_do {p1.begin_vote <+ [[2, 3]]}
    p1.sync_do {p1.ratio <+ [[2, 0.5]]} # Will need 2 of the 3 votes
    p1.sync_do {p1.cast_vote <+ [[2, :A, 'Obama']]}
    p1.sync_do {p1.cast_vote <+ [[2, :B, 'McCain']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[2, :C, 'Obama']], 
                             p1.result.tabname)
    basic_checks(2, :success, 'Obama', resps)
    
    p1.stop
  end

  def test_ratio_voter_fail
    p1 = RatioVotingBloom.new
    p1.run_bg
    
    # Test failure given a ratio of 1
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.ratio <+ [[1, 1]]}
    p1.sync_do {p1.cast_vote <+ [[1, :A, 'Obama']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, :B, 'McCain']], 
                             p1.result.tabname)
    basic_checks(1, :fail, nil, resps)

    # Test failure given a ratio of 0.5
    p1.sync_do {p1.begin_vote <+ [[2, 3]]}
    p1.sync_do {p1.ratio <+ [[2, 0.5]]} # Will need 2 of the 3 votes
    p1.sync_do {p1.cast_vote <+ [[2, :A, 'Obama']]}
    p1.sync_do {p1.cast_vote <+ [[2, :B, 'McCain']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[2, :C, 'Nader']], 
                             p1.result.tabname)
    basic_checks(2, :fail, nil, resps)

    p1.stop
  end

  def test_unanimous_voter
    p1 = UnanimousVotingBloom.new
    p1.run_bg

    # Test success
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.cast_vote <+ [[1, :A, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, :B, 'Obama', 'second']], 
                             p1.result.tabname)
    basic_checks(1, :success, 'Obama', resps)

    # Check accumulated votes/notes
    assert_equal(['Obama', 'Obama'], resps.first[3])
    assert_equal(true, resps.first[4].include?('second'))
    assert_equal(true, resps.first[4].include?('first'))

    # Test fail
    p1.sync_do {p1.begin_vote <+ [[2, 2]]}
    p1.sync_do {p1.cast_vote <+ [[2, :A, 'Obama']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[2, :B, 'McCain']], 
                             p1.result.tabname)
    basic_checks(2, :fail, nil, resps)
    
    p1.stop
  end

  def test_majority_voter
    p1 = MajorityVotingBloom.new
    p1.run_bg

    # Test success in a 2 agent case (need both)
    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.cast_vote <+ [[1, :A, 'Obama', 'first']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, :B, 'Obama', 'second']], 
                             p1.result.tabname)
    basic_checks(1, :success, 'Obama', resps)

    # Check accumulated votes/notes
    assert_equal(['Obama', 'Obama'], resps.first[3])
    assert_equal(true, resps.first[4].include?('second'))
    assert_equal(true, resps.first[4].include?('first'))

    # Test success in a 4 agent case (need 3)
    p1.sync_do {p1.begin_vote <+ [[2, 4]]}
    p1.sync_do {p1.cast_vote <+ [[2, :A, 'Obama']]}
    p1.sync_do {p1.cast_vote <+ [[2, :B, 'Obama']]}
    p1.sync_do {p1.cast_vote <+ [[2, :C, 'McCain']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[2, :D, 'Obama']], 
                             p1.result.tabname)
    basic_checks(2, :success, 'Obama', resps)

    # Check accumulated votes/notes
    assert_equal(3, resps.first[3].count('Obama'))
    assert_equal(1, resps.first[3].count('McCain'))

    # Test fail in a 2 agent case (didn't get 2)
    p1.sync_do {p1.begin_vote <+ [[3, 2]]}
    p1.sync_do {p1.cast_vote <+ [[3, :A, 'Obama']]}
    resps = p1.sync_callback(p1.cast_vote.tabname, [[3, :B, 'McCain']], 
                             p1.result.tabname)
    basic_checks(3, :fail, nil, resps)
    
    p1.stop
  end
end
