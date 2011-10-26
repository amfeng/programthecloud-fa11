require 'rubygems'
require 'bud'
require 'test/unit'
require '../quorum_kvs'

class TestQuorum < Test::Unit::TestCase
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
    resps = p1.sync_callback (:commit_request, [[5]], :commit_response)
    assert_equal([["C"]], resps)

    p1.stop
  end
end
