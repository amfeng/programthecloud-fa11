require 'rubygems'
require 'bud'
require 'delivery/delivery'

module PaxosProtocol
  include MembershipProtocol
  state do
    # The client sends the Paxos master (proposers) a request, which it then tries to
    # get the rest of the nodes (acceptors) to agree on.
    interface input, :request, [:request]
  end
end

module PaxosInternalProtocol
  include DeliveryProtocol
end

module Paxos
  include PaxosProtocol
  include PaxosInternalProtocol

  state do
  end

  # At a client's request, send a PREPARE request to a majority of acceptors
  bloom :prepare do
  end

  # When the proposor receives a response to its PREPARE request from a majority
  # of the acceptors, send an ACCEPT request to each of those acceptors with value
  # v, where v is the value of the highest-numbered proposal among the responses, 
  # or is the value that the client requested if the responses reported no
  # proposals
  bloom :propose do
  end

  # If an acceptor receives a PREPARE request with number n greater than that of
  # any PREPARE request to which it has already responded, responds with a promise
  # not to accept any future proposals numbered less than n and with the highest
  # numbered proposal to which it has already accepted (if any).
  bloom :promise do
  end

  # If an acceptor receives an ACCEPT request for a proposal numbered n, it accepts
  # the proposal unless it has already promised not to in the :promise phase.
  bloom :accept do
  end
end
