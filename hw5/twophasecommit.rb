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
    table :active_ballot, ballots.schema
  end

  bootstrap do
    active <= [[:true]]
  end

  bloom :decide do
    # Only reply to ballots if participant is currently active
    active_ballot <= (ballot * active).lefts
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
  import VotingMaster => vm
  import Multicast => rm

  state do
    # Keep track of ident -> host, since members table holds the 
    # reverse only
    table :participants, [:reqid, :partid] => [:host]
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

  bloom :broadcast do
    # TODO: Why do we need both voting and broadcasting? Voting looks like
    # it already broadcasts on begin_vote
    # Broadcast commit_request to all the participants in the member table
    rm.send_mcast <= commit_request { |r| [r.reqid, :commit_request] }
  end 

  bloom :reply do
    # If all the participants send a "Yes to commit" ack back - send a "commit"
    # request to all the participants
    # TODO

    # Once all the participants send back a "commited" ack, then the coordinator
    # can put a commit message in the commit_response output interface
    # TODO

    # TODO: Failure detection
  end
end
