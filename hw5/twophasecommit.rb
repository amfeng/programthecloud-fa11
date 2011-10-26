require 'rubygems'
require 'bud'

module TwoPCCoordinatorProtocol
  state do
    interface input, :commit_request, [:reqid] => []
    interface output, :commit_response, [:reqid] => [:status]
  end
end

module AgreementConfigProtocol
  state do
    interface input :add_participant, [:reqid, :partid] => [:host]
    interface input :delete_participant, [:reqid, :partid]
    interface input :pause_participant, [:reqid, :partid]
    interface input :resume_participant, [:reqid, :partid]
    interface output :ack, [:reqid]
  end
end

module ParticipantControlProtocol
  state do
    channel :pause, [:@to, :from, :reqid]
    channel :unpause, [:@to, :from, :reqid]
    channel :control_ack, [:@to, :from, :reqid]
  end
end

module TwoPCParticipant
  include VotingAgent
  include ParticipantControlProtocol

  state do
    table :active, [] => [:active]
    table :active_ballot, ballot.schema
  end

  bootstrap do
    active <= [[:true]]
  end

  bloom :decide do
    # Only reply to ballots if participant is currently active
    active_ballot <= (ballot * active).lefts
    # If participant active, then reply back saying "yes" to commit

    # FIXME: Make this use ReliableDelivery instead
    cast_vote <= active_ballot { |b| [b.ident, :yes] } 
  end

  bloom :control do
    active <- pause { |p| [:true] }
    active <+ unpause { |p| [:true] }

    control_ack <+ pause { |p| [p.from, p.to, p.reqid] }
    control_ack <+ unpause { |p| [p.from, p.to, p.reqid] }
  end
end

module TwoPCCoordinator
  include TwoPCCoordinatorProtocol
  include AgreementConfigProtocol
  include ParticipantControlProtocol
  import TwoPCVotingCounting => vc
  import ReliableMulticast => rm

  state do
    # Keep track of ident -> host, since members table holds the 
    # reverse only
    table :participants, [:reqid, :partid] => [:host]
    scratch :phase_one_response, result.schema
    scratch :phase_two_response, result.schema
  end

  bloom :participant_control do
    # Adding participants
    participants <= add_participant
    rm.add_member <= add_participant { |p| [p.host, p.partid] }
    ack <+ add_participant { |r| r.reqid }

    # Pausing participants 
    pause <= (pause_participant * participants).pairs(:partid => :partid) { |r, p|
      [p.host, ip_port, r.reqid]
    }

    # Unpausing participants 
    unpause <= (resume_participant * participants).pairs(:partid => :partid) { |r, p|
      [p.host, ip_port, r.reqid]
    }

    ack <= control_acks

    # Deleting participants
    rm.member <- (delete_participant * participants).pairs(:partid => :partid) { |r, p| [p.host, p.partid] }
    participants <- (delete_participant * participants).paird(:partid => :partid) { |p| [p.reqid, p.partid, p.host] }
    ack <+ delete_participant { |r| r.reqid }
    
  end

  bloom :done_mcast do
    rm.mcast_done <= pipe_sent {|p| [p.dst, p.ident, p.payload] }
  end

  bloom :broadcast do
    # Reliably broadcast commit_request to all the participants 
    vc.begin_votes <= commit_request { |r| [r.reqid, :phase_one, rm.members.length, 5] }
    rm.send_mcast <= commit_request { |r| [r.reqid, :commit_request] }
  end 

  bloom :reply do
    # If all participants can commit, decide to commit. Else, abort.
    vm.phase_one_acks <= rm.mcast_done # FIXME
    commit_response <= vm.phase_two_voting_result

    # TODO: Broadcast decision to the nodes
    vm.phase_two_acks <= rm.mcast_done # FIXME

    # TODO: Clean up once we have received all the acks for
    # Phase 2
    phase_two_response <= phase_two_voting_result
  end
end
