require 'rubygems'
require 'bud'
require 'test/unit'
require 'voting'

class TestVoting < Test::Unit::TestCase
  class VotingBloom
    include Bud
    include RatioVoteCounter
  end

  # def test_unanimous_success
  #   p1 = VotingBloom.new
  #   p1.run_bg

  #   p1.sync_do {p1.begin_vote <+ [[1, 2]]}
  #   p1.sync_do {p1.ratio <+ [[1, 1]]}
  #   p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'first']]}
    
  #   resps = p1.sync_callback(p1.cast_vote.tabname, [[1, 'Obama', 'second']], 
  #                            p1.result.tabname)
    
  #   assert_equal(1, resps.first[0])
  #   assert_equal(:success, resps.first[1])
  #   assert_equal('Obama', resps.first[2])
  #   assert_equal(['Obama', 'Obama'], resps.first[3])
  #   assert_equal(true, resps.first[4].include?('second'))
  #   assert_equal(true, resps.first[4].include?('first'))
  #   p1.stop
  # end

  def test_unanimous_fail
    p1 = VotingBloom.new
    p1.run_bg

    p1.sync_do {p1.begin_vote <+ [[1, 2]]}
    p1.sync_do {p1.ratio <+ [[1, 1]]}
    p1.sync_do {p1.cast_vote <+ [[1, 'Obama', 'first']]}
    
    resps = p1.sync_callback(p1.cast_vote.tabname, [[1, 'McCain', 'second']], 
                             p1.result.tabname)
    
    assert_equal(1, resps.first[0])
    assert_equal(:fail, resps.first[1])
    p1.stop
  end
end
