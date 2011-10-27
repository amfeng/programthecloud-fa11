require 'rubygems'
require 'bud'
require 'voting/voting'
require 'delivery/reliable_delivery'
require 'membership/membership'
require 'new_voting'

module TwoPCCoordinatorProtocol
  state do
    interface input, :commit_request, [:reqid] => []
    interface output, :commit_response, [:reqid] => [:status]
  end
end

module AgreementConfigProtocol
  state do
    interface input, :add_participant, [:reqid, :partid] => [:host]
    interface input, :delete_participant, [:reqid, :partid]
    interface input, :pause_participant, [:reqid, :partid]
    interface input, :resume_participant, [:reqid, :partid]
    interface output, :acks, [:reqid]
  end
end

module ParticipantControlProtocol
  include ReliableDelivery 
  state do
    channel :to_pause, [:@to, :from, :reqid]
    channel :to_unpause, [:@to, :from, :reqid]
    channel :control_ack, [:@to, :from, :reqid]
  end
end

module TwoPCParticipant
  include ParticipantControlProtocol

  state do
    table :active, [] => [:active]
  end

  bootstrap do
    active <= [[:true]]
  end

  bloom :decide do
    # Only reply to ballots if participant is currently active
    #active_ballot <= (ballot * active).lefts
    # If participant active, then reply back saying "yes" to commit

    # FIXME: Make this use ReliableDelivery instead
    #cast_vote <= active_ballot { |b| [b.ident, :yes] } 
    pipe_in <= pipe_out { |p| [p.src, p.dst, p.ident, "yes"] }
  end

  bloom :control do
    active <- to_pause { |p| [:true] }
    active <+ to_unpause { |p| [:true] }

    control_ack <~ to_pause { |p| [p.from, p.to, p.reqid] }
    control_ack <~ to_unpause { |p| [p.from, p.to, p.reqid] }
  end
end

module TwoPCCoordinator
  include TwoPCCoordinatorProtocol
  include AgreementConfigProtocol
  include ParticipantControlProtocol
  include StaticMembership
  import TwoPCVoteCounting => :vc

  state do
    # Keep track of ident -> host, since members table holds the 
    # reverse only
    table :participants, [:reqid, :partid] => [:host]
    scratch :phase_one_response, [:reqid] => [:value]
    scratch :phase_two_response, [:reqid] => [:value]
  end

  bloom :participant_control do
    # Adding participants
    participants <= add_participant
    add_member <= add_participant { |p| [p.host, p.partid] }
    acks <+ add_participant { |r| [r.reqid] }

    # Pausing participants 
    to_pause <~ (pause_participant * participants).pairs(:partid => :partid) { 
      |r, p| [p.host, ip_port, r.reqid]
    }

    # Unpausing participants 
    to_unpause <~ (resume_participant * participants).pairs(:partid => :partid) { 
      |r, p| [p.host, ip_port, r.reqid]
    }

    acks <= control_ack
  end

  bloom :broadcast do
    # Reliably broadcast commit_request to all the participants 
    vc.begin_votes <= commit_request { |r| 
      [r.reqid, :phase_one, member.length, 5] 
    }
    #rm.send_mcast <= commit_request { |r| [r.reqid, :commit_request] }
    pipe_in <= (member * commit_request).pairs { |m, r|
      [m.host, ip_port, r.reqid, :commit_request] unless m.host == ip_port
    }
  end 

  bloom :reply do
    # If all participants can commit, decide to commit. Else, abort.
    vc.phase_one_acks <= pipe_out { |p| [p.src, p.ident, p.payload] }
    commit_response <= vc.phase_one_voting_result
    stdio <~ commit_response.inspected

    # Broadcast decision to the nodes
    #rm.send_mcast <= (commit_request * commit_response)
    #pipe_in <= pipe_out

    #vc.phase_two_acks <= rm.mcast_done # FIXME

    # TODO: Clean up once we have received all the acks for
    # Phase 2
    #phase_two_response <= vc.phase_two_voting_result
  end
end
