require 'rubygems'
require 'bud'
require 'delivery/delivery'
require 'membership/membership'

module PaxosProtocol
  include MembershipProtocol
  state do
    # The client sends the Paxos master (proposer) a request, which it then tries to
    # get the rest of the nodes (acceptors) to agree on.
    interface input, :request, [:id] => [:value]

    # The proposer sends the client back a result, after all of the acceptors have
    # accepted the new value
    interface output, :result, [:id]
  end
end

module PaxosInternalProtocol
  include DeliveryProtocol
end

module Paxos
  include PaxosProtocol
  include PaxosInternalProtocol

  bootstrap do
    # Put negative proposal numbers into acceptor state
    accepted_proposal <= [-1]
    accepted_prepare <= [-1, nil]
  end

  state do
    # == Proposer state ==
    # Counter to have distinct, increasing number n's for each proposal
    # from this proposer
    # TODO: How to have distinct, incrementing counters for different proposers?
    table :counter, [] => [:n]

    # Table to keep track of current requests
    table :requests, [:n] => [:stage, :value]

    # Table to keep track of promises made to the proposer from various acceptors
    # Note: Acceptors also respond with the highest-number proposal they have
    # already accepter, if any (:note)
    table :promises, [:n, :note, :member]

    # Table to keep track of accepts made to the proposer from various acceptors
    table :accepts, [:n, :member]

    # PREPARE requests to send out, at a clients' request
    scratch :to_prepare, [:n, :value]

    # Proposals to send out, after the PREPARE stage has enough responses
    scratch :to_propose, [:n, :value]

    # == Acceptor state ==
    # Highest numbered PREPARE request the acceptor has ever responded to
    scratch :accepted_prepare, [] => [:n]

    # Highest numbered proposal the acceptor has ever accepted
    scratch :accepted_proposal, [] => [:n, :value]

    scratch :to_promise, pipe_in.schema
    scratch :to_accept, pipe_in.schema
  end

  # At a client's request, send a PREPARE request to a majority of acceptors
  bloom :prepare do
    # Increment n counter
    counter <+- counter { |c| [c.n + 1] }

    to_prepare <= (request * counter).pair { |r, c| [c.n, r.value] }
    requests <= to_prepare { |p| [p.n, :prepare, p.value] }

    # Send PREPARE request to all acceptors
    # TODO: Later, send to only a majority of acceptors?
    # FIXME: Each timestep could have many requests, increment counter
    # for each one
    pipe_in <= (member * to_prepare).combos { |m, p|
      # :ident of the message is the combination of message type plus
      # the number n of the proposal
      [m.host, ip_port, [:prepare, p.n], nil]
    }
  end

  # When the proposor receives a response to its PREPARE request from a majority
  # of the acceptors, send an ACCEPT request to each of those acceptors with value
  # v, where v is the value of the highest-numbered proposal among the responses, 
  # or is the value that the client requested if the responses reported no
  # proposals
  bloom :propose do
    # Save promises into persistent storage
    promises <= pipe_out { |p|
      [p.ident[1], p.payload, p.src] if p.ident[0] == :promise
    }

    # TODO: Count number of promises, if majority, send out proposal
    # TODO: Determine value to send out depending on responses

    requests <+- (requests * to_propose).lefts(:n => :n) { |r| 
      [r.n, :propose, r.value]
    }

    # Send ACCEPT request to all acceptors
    # TODO: Later, send to only a majority of acceptors?
    pipe_in <= (member * to_propose).pair { |m, r, c|
      # :ident of the message is the combination of message type plus
      # the number n of the proposal
      [m.host, ip_port, [:propose, p.n], p.value]
    }
  end

  # Count the number of ACCEPT request responses received, if receive from
  # a majority of the acceptors, finish the request
  bloom :finish do
    # Save accepts into persistent storage
    accepts <= pipe_out { |p|
      [p.ident[1], p.src] if p.ident[0] == :accept
    }

    # TODO: Count number of acceptances, if majority, tell client we're done
  end

  # If an acceptor receives a PREPARE request with number n greater than that of
  # any PREPARE request to which it has already responded, responds with a promise
  # not to accept any future proposals numbered less than n and with the highest
  # numbered proposal to which it has already accepted (if any).
  bloom :promise do
    to_promise <= (pipe_out * accepted_proposal * accepted_prepare).combos  { |p, a, pr|
      [p.src, ip_port, [:promise, p.n], a.value] if p.n >= pr.n
    }

    # Update the highest numbered PREPARE request we have ever responded to
    accepted_prepare <+- to_promise { |pr| [pr.ident[1]] }

    # Send promise
    pipe_in <= to_promise
  end

  # If an acceptor receives an ACCEPT request for a proposal numbered n, it accepts
  # the proposal unless it has already promised not to in the :promise phase.
  bloom :accept do
    to_accept <= (pipe_out * accepted_prepare).pairs { |p, pr| 
      [p.src, ip_port, [:accept, p.n], nil] if p.n >= pr.n    
    }

    # Update the highest numbered proposal we have ever accepted
    accepted_proposal <+- to_accept { |a| [pr.ident[1]] }
    
    # Send accept
    pipe_in <= to_accept
  end
end
