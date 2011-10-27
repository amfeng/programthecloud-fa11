require 'rubygems'
require 'bud'
require 'test/unit'
require 'twophasecommit'

class Test2PC < Test::Unit::TestCase
  class TwoPCTest
    include Bud
    include TwoPCParticipant
    include TwoPCCoordinator
  end

  def test_twopc
    p1 = TwoPCTest.new()
    p1.run_bg

    # Initialize many participants
    p1.sync_do {p1.add_participant <+ [[1, 1, 'localhost:54320'],
                                       [2, 2, 'localhost:54321'],
                                       [3, 3, 'localhost:54322'],
                                       [4, 4, 'localhost:54323']]}
    
    # Broadcast a commit request - it should succeed
    resps = p1.sync_callback(:commit_request, [[5]], :commit_response)
    assert_equal([["C"]], resps)
    
    # # Pause participant 1
    # p1.sync_callback {p1.pause_participant <+ [[6,1]]}
    
    # # Broadcast a commit request - it should fail
    # resps = p1.sync_callback (:commit_request, [[7]], :commit_response)
    # assert_equal([["A"]], resps)

    # # Delete participant 1 now
    # p1.sync_callback {p1.delete_participant <+ [[8,1]]}

    # # Broadcast a commit request - it should succeed
    # resps = p1.sync_callback (:commit_request, [[9]], :commit_response)
    # assert_equal([["C"]], resps)

    # Pause a participant 2
    p1.sync_callback {p1.pause_participant <+ [[10,2]]}
    
    # Broadcast a commit request - it should fail
    resps = p1.sync_callback(:commit_request, [[11]], :commit_response)
    assert_equal([["A"]], resps)

    # Resume participant 2
    p1.sync_callback {p1.pause_participant <+ [[12,2]]}
    
    # Broadcast a commit request - it should succeed
    resps = p1.sync_callback(:commit_request, [[13]], :commit_response)
    assert_equal([["C"]], resps)

    p1.stop
  end
end
