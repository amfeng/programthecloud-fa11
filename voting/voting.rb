# @abstract VoteCounterProtocol is the interface for vote counting. 
# A vote counting protocol should subclass VoteCounterProtocol.
module VoteCounterProtocol
  state do
    # On the client side, tell the vote counter to start counting votes
    # for a specific ballot.
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] num_votes the number of votes that will be cast (this number will remain static throughout the vote)
    interface input :begin_vote, [:ballot_id] => [:num_votes]

    # On the client side, send votes to be counted
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Object] vote specific vote
    # @param [String] note any extra information to provide along with the vote
    interface input :cast_vote, [:ballot_id, :vote, :note]

    # Returns the result of the vote once
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Symbol] status status of the vote, :success, :fail
    # @param [Object] result outcome of the vote, contents depend on :vote field of cast_vote input
    # @param [Array] votes an aggregate of all of the votes cast
    # @param [Array] notes an aggregte of all of the notes send
    interface output, :result, [:ballot_id] => [:status, :result, :votes, :notes]
  end
end

# RatioVoteCounter is an implementation of the VoteCounterProtocol in which a floating point ratio is provided to specify what ratio of the total number of votes is needed for a "winning" vote. Note: the calculation is rounded up, ex. votes_needed = ceil((ratio) * num_votes).
# @see RatioVoteCounter implements VoteCounterProtocol
module RatioVoteCounter
  include VoteCounterProtocol
  state do
    # On the client side, tell the vote counter what ratio to set. This ratio must be set before the vote starts.
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] ratio floating point number for the percentage of votes needed to "win"
    interface input :ratio, [:ballot_id] => [:ratio]
  end
end

# UnanimousVoteCounter is a specific case of RatioVoteCounter, where the ratio is 1.
# @see UnanimousVoteCounter extends RatioVoteCounter
module UnanimousVoteCounter
  include RatioVoteCounter
end

# MajorityVoteCounter is an implementation of the VoteCounterProtocol, where the number of votes needed for a majority is floor(ratio * num_members) + 1
# @see MajorityVoteCounter implements VoteCounterProtocol
module MajorityVoteCounter
  include VoteCounterProtocol
end
