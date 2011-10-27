require 'rubygems'
require 'bud'
require 'test/unit'
require 'twophasecommit'

class Test2PC < Test::Unit::TestCase
  class Coordinator
    include Bud
    include TwoPCCoordinator

    bootstrap do
      add_participant <= [[1, 1, "localhost:54320"]]
    end
  end

  class Participant
    include Bud
    include TwoPCParticipant
  end

  def test_twopc
    coord = Coordinator.new(:port => 12345)
    p1 = Participant.new(:port => 54320)

    coord.run_bg
    p1.run_bg

    # Broadcast a commit request - it should succeed
    coord.sync_do { coord.commit_request <+ [[1]] }
    puts p1.pipe_out.inspected
    #resps = coord.sync_callback(:commit_request, [[5]], :pipe_out)
    #assert_equal([["C"]], resps)
    
    # # Pause participant 1
    # p1.sync_callback {p1.pause_participant <+ [[6,1]]}
    
    # # Broadcast a commit request - it should fail
    # resps = p1.sync_callback(:commit_request, [[7]], :commit_response)
    # assert_equal([["A"]], resps)

    # # Delete participant 1 now
    # p1.sync_callback {p1.delete_participant <+ [[8,1]]}

    # # Broadcast a commit request - it should succeed
    # resps = p1.sync_callback(:commit_request, [[9]], :commit_response)
    # assert_equal([["C"]], resps)

    # Pause a participant 2
    #p1.sync_callback {p1.pause_participant <+ [[10,2]]}
    
    # Broadcast a commit request - it should fail
    #resps = p1.sync_callback(:commit_request, [[11]], :commit_response)
    #assert_equal([["A"]], resps)

    # Resume participant 2
    #p1.sync_callback {p1.pause_participant <+ [[12,2]]}
    
    # Broadcast a commit request - it should succeed
    #resps = p1.sync_callback(:commit_request, [[13]], :commit_response)
    #assert_equal([["C"]], resps)

    #p1.stop
  end
end
