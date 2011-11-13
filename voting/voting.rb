# @abstract VoteCounterProtocol is the interface for vote counting. 
# A vote counting protocol should subclass VoteCounterProtocol.
module VoteCounterProtocol
  state do
    # On the client side, tell the vote counter to start counting votes
    # for a specific ballot.
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] num_votes the number of votes that will be cast 
    # (this number will remain static throughout the vote)
    interface input :begin_vote, [:ballot_id] => [:num_votes]

    # On the client side, send votes to be counted
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Object] candidate specific candidate we're voting for
    # @param [String] note any extra information to provide along with the vote
    interface input :cast_vote, [:ballot_id, :candidate, :note]

    # Returns the result of the vote once
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Symbol] status status of the vote, :success, :fail
    # @param [Object] result outcome of the vote, contents depend on 
    # :vote field of cast_vote input
    # @param [Array] candidates an aggregate of candidates of all of the 
    # votes cast
    # @param [Array] notes an aggregte of all of the notes send
    interface output, :result, [:ballot_id] => [:status, :result, :candidates, :notes]
  end
end

# CountVoteCounter is an implementation of the VoteCounterProtocol in which
# a certain number of required votes for a "winning" candidate is provided 
# directly.
# @see CountVoteCounter implements VoteCounterProtocol
module CountVoteCounter
  include VoteCounterProtocol
  state do
    # On the client side, tell the vote counter how many votes are required
    # for a winning vote. Note that the ballot must already be initialized
    # via begin_vote before sending this in.
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] num_required the number of votes required for a 
    # winning vote (ex. unanimous = number of total votes)
    interface input :required_votes, [:ballot_id] => [:num_required]  

    # ===Private State===

    # Table to keep track of ballots that have been initialized via
    # begin_vote, but have not had a required number of votes sent in yet.
    table :preballot, begin_vote.schema

    # Table to keep track of ballots that have been initialized via
    # begin_vote, but have not had a required_num sent in yet.
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] num_required see :num_votes in begin_vote
    table :ballot, [:ballot_id] => [:num_votes, :num_required]

    # Table to keep track of votes that have been cast
    table :vote, cast_vote.schema

    # Intermediate table to keep track of counts for each candidate,
    # per ballot
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Object] candidate the specific candidate
    # @param [Number] num how many votes a candidate has
    scratch :counts, [:ballot_id, :candidate] => [:num]

    # Intermediate table to keep track of which candidates have enough
    # votes to make them "winning" candidates
    scratch :enough, [:ballot_id, :vote]
    scratch :extra, [:ballot_id] => [:votes, :notes]
  end

  bloom :start do
    # Since we have two "rounds" of initializing a ballot (one that matches
    # the VoteCounterProtocol, and that one that passes in the number of
    # required votes for a "winning" candidate), we need two tables
    preballot <= begin_vote
    ballot <= (preballot * required_votes).pairs(:ballot_id => :ballot_id) { |p, r|
      [p.ballot_id, p.num_votes, r.num_required]     
    }
  end

  bloom :save_votes do
    vote <= cast_vote
  end

  bloom :count_votes do
    # Count all of the votes for each ballot
    counts <= vote.group([:ballot_id, :vote], count()) 

    # Find the ballots that have vote counts >= the ballot's required number
    # of votes for a "winning" candidate 
    enough <= (counts * ballot).pairs(:ballot_id => :ballot_id) { |c, b|
      [c.ballot_id, c.vote] if c.num >= b.num_required
    } 
    
    # Add extra votes/notes data into the result
    extra <= vote.group([:ballot_id], accum(:vote), accum(:note))
    result <= (enough * extra).pairs(:ballot_id => :ballot_id) { |w, e|
      [w.ballot_id, :success, w.vote, extra.votes, extra.notes]
    }
  end
end

# RatioVoteCounter is an implementation of the VoteCounterProtocol in which
# a floating point ratio is provided to specify what ratio of the total number
# of votes is needed for a "winning" candidate. Note: the calculation is 
# rounded up, ex. votes_needed = ceil((ratio) * num_votes).
# @see RatioVoteCounter implements VoteCounterProtocol
module RatioVoteCounter
  include CountVoteCounter
  state do
    # On the client side, tell the vote counter what ratio to set. This 
    # ratio must be set before the vote starts.
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] ratio floating point number for the percentage of votes 
    # needed for a candidate to "win"
    interface input :ratio, [:ballot_id] => [:ratio]
  end

  bloom :delegate do
    required_votes <= (ratio * ballot).pairs(:ballot_id => :ballot_id) { |r, b| 
      [r.ballot_id, r.ratio * b.num_votes]
    }
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

module UniqueVoteCounter
end

