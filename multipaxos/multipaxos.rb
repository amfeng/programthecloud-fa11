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
    # get the rest of the nodes (acceptors) to agree on. It is assumed that a
    # interface user will only insert a request after they have recieved a result
    # for a previous request.
    # @param [String] ident is a unique identifier attached to this request
    # @param [Object] value is the proposed value for this request
    interface input, :request, [:ident] => [:value]

    # The proposer sends the client back a result, after all of the acceptors have
    # accepted the new value
    # @param [String] ident is a unique identifier attached to this request
    # @param [String] status is the success/failure status from the vote couting module
    # @param [Object] value that was agreed upon by the algorithm
    interface output, :result, [:ident] => [:status, :value]
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
    accepted_proposal <= [[nil, -1, -1, nil]]
    accepted_prepare <= [[nil, -1, -1]]
    counter <= [[rand(100000), ip_port]]
    round <= [[0]]
    unstable <= [[true]]
  end

  state do
    # == Proposer state ==
    # Counter of the form [n, ip_address], where 'n' is an increasing
    # integer (starting at a random integer between 0 and 100k), and
    # the ip_address references this proposer. The ip address gives
    # distinction between overlapping 'n' between separate proposers.
    table :counter, [] => [:n, :addr]

    # Integer counter that indicates the current round of voting.
    table :round, [] => [:n]

    # Table to keep track of current requests
    table :requests, [:n, :rnd] => [:stage, :value]

    # Table to determine whether this proposer is in a stable state. It is assumed
    # that this table is bootstraped with any value, and that the value will be
    # removed when a proposer has reached steady state.
    table :unstable, [] => [:value]

    # Table to indicate that this proposer has reached steady state. This table
    # starts out empty, and is populated with any value when the proposer reaches
    # steady state.
    table :stable, [] => [:value]

    # Temporary storage to hold the next PREPARE and PROPOSE messages to send out
    scratch :to_prepare, [:n, :rnd, :value]
    scratch :to_propose, [:n, :rnd, :value]
    table :promises, [:n, :rnd, :value]
    scratch :promise_max, [:n, :rnd, :value]

    # Temporary storage with the calculated highest-numbered proposal sent back
    # by the acceptors in the PREPARE phase after a majority has been reached
    scratch :result_max, [:ballot_id] => [:maximum]

    # Temporary storage with the calculated value to send the PROPOSE message
    # with, based on the highest-numbered proposal sent back in the PREPARE phase
    scratch :result_values, [:n, :rnd] => [:value]

    # == Acceptor state ==
    # Highest numbered PREPARE request the acceptor has ever responded to
    # (saving both the prepare number and the round number)
    table :accepted_prepare, [:key] => [:n, :rnd]

    # Highest numbered proposal the acceptor has ever accepted
    # (saving both the proposal number and the round number)
    table :accepted_proposal, [:key] => [:n, :rnd, :value]

    # Temporary storage to hold the next PROMISE and ACCEPT messages to send out
    scratch :to_promise, pipe_in.schema
    scratch :to_accept, pipe_in.schema
  end

  bloom :increments do
    # Increment n counter whenver there is a request
    counter <+- (counter * request).lefts { |c| [c.n + 1, c.addr] }

    # Increment the round counter whenever we recieve a new request.
    round <+- (round * request).lefts { |r| [r.n + 1] }
  end

  # When a client submits a request, if the preparer has not reached a stable state,
  # then send a PREPARE request to all of the acceptors.
  bloom :prepare do
    to_prepare <= (request * counter * unstable * round).combos {
      |r, c, u, d| [[c.n, c.addr], d.n, r.value]
    }
    requests <= to_prepare { |p| [p.n, p.rnd, :prepare, p.value] }

    # Start vote counting for this stage
    vc.begin_vote <+ to_prepare { |p|
      # :ballot_id is a combination of n, the round, and the current stage
      [["promise", p.n, p.rnd], member.length]
    }

    # Send PREPARE request to all acceptors
    pipe_in <= (member * to_prepare).combos { |m, p|
      # :ident of the message is the combination of message type plus
      # the number n of the proposal, and the current round
      [m.host, ip_port, [:prepare, p.n, p.rnd], nil]
    }
  end

  # When the proposor receives a response to its PREPARE request from a majority
  # of the acceptors, send an ACCEPT request to each of those acceptors with value
  # v, where v is the value of the highest-numbered proposal among the responses,
  # or is the value that the client requested if the responses reported no
  # proposals
  bloom :propose do
    # Pass promises into vote counter, if its round is equal to this round.
    vc.cast_vote <+ (pipe_out * round).pairs { |p, d|
      [p.ident, p.src, nil, p.payload] if p.ident[0] == "promise" and p.ident[2] + 1 == d.n
    }

    promises <= (pipe_out * round).pairs { |p, d|
      [p.ident[1], p.ident[2], p.payload] if p.ident[0] == "promise" and p.ident[2] + 1 == d.n
    }

    promise_max <= promises.group([:n, :rnd, :value], max(:n))

    to_propose <= (vc.result * promise_max * requests).pairs {|r, p, rq|
      if p.value == nil
        [p.n, p.rnd, rq.value] if rq.n == p.n and rq.rnd == p.rnd
      else
        [p.n, p.rnd, p.value]
      end
    }

    promises <- (promises * to_propose).lefts

    # If we are currently in an unstable state, when a value comes
    # up for proposal, enter steady state. Remove any value from
    # the table "unstable" and add a value to "stable"
    stable <+ to_propose {|p| [p.value] if stable.empty?}
    unstable <- (unstable * to_propose).lefts {|u| [u.value]}

    # Start vote counting for this stage
    vc.begin_vote <+ to_propose { |p|
      # :ballot_id is a combination of n and the current stage
      [["accept", p.n, p.rnd], member.length]
    }

    # Update the current stage in the requests table
    requests <+- (requests * to_propose).lefts(:n => :n) { |r|
      [r.n, r.rnd, :propose, r.value]
    }

    # Send ACCEPT request to all acceptors
    pipe_in <= (member * to_propose).pairs { |m, p|
      # :ident of the message is the combination of message type plus
      # the number n of the proposal
      [m.host, ip_port, [:propose, p.n, p.rnd], p.value]
    }
  end


  # In the case that the proposer is in a stable state, populate the to_propose field
  # and execute its associated rules as defined in the "propose" block.
  bloom :stable_propose do
    # If we are in a stable mode, propose the requested value with the current
    # counter (which autoincrements above).
    to_propose <= (request * counter * stable * round).combos {|r, c, s, d| [[c.n, c.addr], d.n, r.value]}
  end

  # Count the number of ACCEPT request responses received, if receive from
  # a majority of the acceptors, finish the request
  bloom :finish do
    # Pass accepts into vote counter
    vc.cast_vote <= (pipe_out * round).pairs { |p, d|
      [p.ident, p.src, nil, p.payload] if p.ident[0] == "accept" and p.ident[2] + 1 == d.n
    }

    #stdio <~ [[result.inspected]]

    # Count number of acceptances, if majority, tell client we're done

    result <= (vc.result * requests).pairs { |r, q|
      [r.ballot_id[1], r.status, q.value] if r.ballot_id[0] == "accept"
    }

    requests <- (requests * result).lefts

  end

  # If an acceptor receives a PREPARE request with number n greater than that of
  # any PREPARE request to which it has already responded, responds with a promise
  # not to accept any future proposals numbered less than n and with the highest
  # numbered proposal to which it has already accepted (if any).
  bloom :promise do
    to_promise <= (pipe_out * accepted_proposal * accepted_prepare).combos  {
      |p, a, pr|
      if pr.rnd == p.ident[2]
        [p.src, ip_port, [:promise, p.ident[1], p.ident[2]], a.value] if p.ident[1][0] >= pr.n[0] and p.ident[0] == "prepare"
      else
        [p.src, ip_port, [:promise, p.ident[1], p.ident[2]], nil] if p.ident[0] == "prepare"
      end
    }

    # Update the highest numbered PREPARE request we have ever responded to
    accepted_prepare <+- to_promise { |pr| [nil, pr.ident[1], pr.ident[2]] }

    # Send promise
    pipe_in <= to_promise
  end

  # If an acceptor receives an ACCEPT request for a proposal numbered n, it accepts
  # the proposal unless it has already promised not to in the :promise phase. Otherwise
  # it sends a nack to the proposer.
  bloom :accept do
    # The case where we have had a prepare phase
    to_accept <= (pipe_out * accepted_prepare).pairs { |p, pr|
      if (pr.rnd == p.ident[2] and p.ident[1][0] >= pr.n[0]) or pr.rnd != p.ident[2]
        [p.src, ip_port, [:accept, p.ident[1], p.ident[2]], nil] if p.ident[0] == "propose"
      end
    }

    # Update the highest numbered proposal we have ever accepted
    # FIXME: Need to add the value of this proposal to accepted_proposal
    accepted_proposal <+- to_accept { |a| [nil, a.ident[1], a.ident[2], nil]}

    # Send accept
    pipe_in <= to_accept
  end
end
