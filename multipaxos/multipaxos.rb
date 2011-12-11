require 'rubygems'
require 'bud'
require 'delivery/delivery'
require 'membership/membership'
require 'voting/voting'

# @abstract PaxosProtocol is the abstract interface for finding concensus in a 
# network of unreliable processes.
module PaxosProtocol
  include MembershipProtocol
  state do
    # The client sends the Paxos master (proposer) a request, which it then tries to
    # get the rest of the nodes (acceptors) to agree on.
    # @param [String] ident is a unique identifier attached to this request
    # @param [Object] value is the proposed value for this request
    interface input, :request, [:ident] => [:value]

    # The proposer sends the client back a result, after all of the acceptors have
    # accepted the new value
    # @param [String] ident is a unique identifier attached to this request
    # @param [String] status is the success/failure status from the vote couting module
    interface output, :result, [:ident] => [:status]
  end
end

module PaxosInternalProtocol
  include DeliveryProtocol
end

module Paxos
  include PaxosProtocol
  include PaxosInternalProtocol
  import MajorityVoteCounter => :vc

  bootstrap do
    # Put negative proposal numbers into acceptor state and
    # initialize the counter to a random number [0, 100k].
    accepted_proposal <= [-1]
    accepted_prepare <= [-1, nil]
    counter <= [rand(100000), ip_port]
    unstable <= [true]
  end

  state do
    # == Proposer state ==
    # Counter of the form [n, ip_address], where 'n' is an increasing
    # integer (starting at a random integer between 0 and 100k), and 
    # the ip_address references this proposer. The ip address gives 
    # distinction between overlapping 'n' between separate proposers.
    table :counter, [] => [:n, :addr]

    # Table to keep track of current requests
    table :requests, [:n] => [:stage, :value]

    # Table to determine whether this proposer is in a stable state. It is assumed
    # that this table is bootstraped with any value, and that the value will be
    # removed when a proposer has reached steady state.
    table :unstable, [] => [:value]

    # Table to indicate that this proposer has reached steady state. This table
    # starts out empty, and is populated with any value when the proposer reaches
    # steady state.
    table :stable, [] => [:value]

    # Temporary storage to hold the next PREPARE and PROPOSE messages to send out
    scratch :to_prepare, [:n, :value]
    scratch :to_propose, [:n, :value]

    # Temporary storage with the calculated highest-numbered proposal sent back
    # by the acceptors in the PREPARE phase after a majority has been reached
    scratch :result_max, [:ballot_id] => [:maximum]

    # Temporary storage with the calculated value to send the PROPOSE message
    # with, based on the highest-numbered proposal sent back in the PREPARE phase
    scratch :result_values, [:n] => [:value]

    # == Acceptor state ==
    # Highest numbered PREPARE request the acceptor has ever responded to
    table :accepted_prepare, [] => [:n]

    # Highest numbered proposal the acceptor has ever accepted
    table :accepted_proposal, [] => [:n, :value]

    # Temporary storage to hold the next PROMISE and ACCEPT messages to send out
    scratch :to_promise, pipe_in.schema
    scratch :to_accept, pipe_in.schema
  end

  # At a client's request, send a PREPARE request to a majority of acceptors
  bloom :prepare do
    # Increment n counter whenver there is a request
    counter <+- (counter * request).lefts { |c| [c.n + 1, c.addr] }

    to_prepare <= (request * counter * unstable).pair { |r, c, u| [[c.n, c.addr], r.value] }
    requests <= to_prepare { |p| [p.n, :prepare, p.value] }

    # Start vote counting for this stage
    vc.begin_vote <= to_prepare { |p|
      # :ballot_id is a combination of n and the current stage
      [[p.n, :prepare], member.length] 
    }

    # Send PREPARE request to all acceptors
    # FIXME: send to only a majority of acceptors (or assume that members contains only the quorum)
    # FIXME: Each timestep could have many requests, increment counter for each one
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
    # Pass promises into vote counter
    vc.cast_vote <= pipe_out { |p|
      [p.ident, p.src, nil, p.payload] if p.ident[0] == :prepare
    }

    # Determine value to send out depending on responses
    result_max <= vc.result.group([:ballot_id], max(:notes))
    result_values <= (result_max * requests).pairs { |m, r|
      if m[1] == -1 
        # If no acceptor accepted another proposal, use the client request
        # value
        [r.n, r.value] if r.n == m.ballot_id[1]
      else
        # Else, use the highest-numbered proposal among the responses
        [m.ballot_id[1], m.maximum] 
      end
    }

    to_propose <= (vc.result * result_values).pairs { |r, v|
      [v.n, v.value] if result.ballot_id == [:prepare, v.n]
    }

    # If we are currently in an unstable state, when a value comes
    # up for proposal, enter steady state. Remove any value from 
    # the table "unstable" and add a value to "stable"
    stable <= to_propose {|p| [p.value] if stable.empty?}
    unstable <- (unstable * to_propose).lefts {|u| [u.value]}

    # Start vote counting for this stage
    vc.begin_vote <= to_propose { |p|
      # :ballot_id is a combination of n and the current stage
      [[p.n, :propose], member.length] 
    }

    # Update the current stage in the requests table
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

  bloom :stable_propose do
    # Pass promises into vote counter
    #vc.cast_vote <= pipe_out { |p|
    #  [p.ident, p.src, nil, p.payload] if p.ident[0] == :prepare
    #}

    # Determine value to send out depending on responses
    #result_max <= vc.result.group([:ballot_id], max(:notes))
    #result_values <= (result_max * requests).pairs { |m, r|
    #  if m[1] == -1 
    #    # If no acceptor accepted another proposal, use the client request
    #    # value
    #    [r.n, r.value] if r.n == m.ballot_id[1]
    #  else
    #    # Else, use the highest-numbered proposal among the responses
    #    [m.ballot_id[1], m.maximum] 
    #  end
    #}

    # If we are in a stable mode, propose the requested value with the current
    # counter (which autoincrements above).
    to_propose <= (requst * counter * stable).combos {|r, c, s| [[c.n, c.addr], r.value]}
    
    # Start vote counting for this stage
    vc.begin_vote <= to_propose { |p|
      # :ballot_id is a combination of n and the current stage
      [[p.n, :propose], member.length] 
    }

    # Update the current stage in the requests table
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
    # Pass accepts into vote counter
    vc.cast_vote <= pipe_out { |p|
      [p.ident, p.src, nil, p.payload] if p.ident[0] == :accept
    }

    # Count number of acceptances, if majority, tell client we're done
    result <= vc.result { |r|
      [r.ballot_id[1], r.status] if r.ballot_id[0] == :accept
    }
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
