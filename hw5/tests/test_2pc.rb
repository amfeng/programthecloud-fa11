require 'rubygems'
require 'bud'
require 'test/unit'
require 'twophasecommit'

class Test2PC < Test::Unit::TestCase
  class Coordinator
    include Bud
    include TwoPCCoordinator

    bootstrap do
      add_participant <= [[1, 1, "localhost:54320"],
                          [2, 2, "localhost:54321"]]
    end
  end

  class Participant
    include Bud
    include TwoPCParticipant
  end

  def test_twopc
    coord = Coordinator.new(:port => 12345)
    p1 = Participant.new(:port => 54320)
    p2 = Participant.new(:port => 54321)

    coord.run_bg
    p1.run_bg
    p2.run_bg

    # Broadcast a commit request - it should succeed
    resps = coord.sync_callback(:commit_request, [[1]], :commit_response)
    5.times { coord.sync_do }
    assert_equal([[1, :C]], resps)
    
    # Pause participant 2
    coord.sync_do {coord.pause_participant <+ [[10,2]]}
    5.times { coord.sync_do }
    
    # Broadcast a commit request - it should fail
    resps = coord.sync_callback(:commit_request, [[11]], :commit_response)
    assert_equal([[11, :A]], resps)

    # Resume participant 2
    coord.sync_do {coord.resume_participant <+ [[12,2]]}
    5.times { coord.sync_do }
    
    # Broadcast a commit request - it should succeed
    resps = coord.sync_callback(:commit_request, [[13]], :commit_response)
    assert_equal([[13, :C]], resps)

  end
end
