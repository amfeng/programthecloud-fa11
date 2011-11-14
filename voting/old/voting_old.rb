# @abstract VoteCounterProtocol is the interface for vote counting. 
# A vote counting protocol should subclass VoteCounterProtocol.
module VoteCounterProtocol
  state do
    # On the client side, tell the vote counter to start counting votes
    # for a specific ballot.
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] num_votes the number of votes that will be cast 
    # (this number will remain static throughout the vote)
    interface input, :begin_vote, [:ballot_id] => [:num_votes]

    # On the client side, send votes to be counted
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Object] candidate specific candidate we're voting for
    # @param [Object] agent identifying features of the agent who cast the vote
    # @param [String] note any extra information to provide along 
    # with the vote
    interface input, :cast_vote, [:ballot_id, :agent, :candidate, :note]

    # Returns the result of the vote once
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Symbol] status status of the vote, :success, :fail
    # @param [Object] result outcome of the vote, contents depend on 
    # :vote field of cast_vote input
    # @param [Array] candidates an aggregate of candidates of all of the 
    # votes cast
    # @param [Array] notes an aggregte of all of the notes send
    interface output, :result, [:ballot_id] => [:status, :result, 
                                                :candidates, :notes]
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
    interface input, :required_votes, [:ballot_id] => [:num_required]  

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

    # Table to keep track of the failed ballots
    table :failed_ballots, [:ballot_id, :num_votes, :num_cast]

    # Intermediate table to keep track of counts for each candidate,
    # per ballot
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Object] candidate the specific candidate
    # @param [Number] num how many votes a candidate has
    scratch :counts, [:ballot_id, :candidate] => [:num]

    # Intermediate table to keep track of number of votes cast for each ballot,
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] num_cast how many votes have been cast for a ballot
    scratch :counts2, [:ballot_id] => [:num_cast]

    # Intermediate table to keep track of number of votes cast and
    # number of ballots required for each ballot,
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] num_votes how many votes will be cast for a ballot
    # @param [Number] num_cast how many votes have been cast for a ballot
    scratch :counts3, [:ballot_id, :num_votes, :num_cast]

    # Intermediate table to keep track of ongoing ballots that
    # have not acquired the required number of votes 
    scratch :counts4, counts3.schema

    # Intermediate table to keep track of which candidates have enough
    # votes to make them "winning" candidates
    scratch :enough, [:ballot_id, :vote]
    scratch :extra, [:ballot_id] => [:votes, :notes]

    # Intermediate table to keep track of ballots that have been completed
    # @param [Number] ballot_id a ballot that has been completed
    scratch :completed_ballots, [:ballot_id] 
  end

  bloom :start do
    # Since we have two "rounds" of initializing a ballot (one that matches
    # the VoteCounterProtocol, and that one that passes in the number of
    # required votes for a "winning" candidate), we need two tables
    preballot <= begin_vote
    ballot <= (preballot * required_votes).pairs(:ballot_id => :ballot_id) {
      |p, r| [p.ballot_id, p.num_votes, r.num_required]     
    }
  end

  bloom :save_votes do
    vote <= cast_vote
  end

  bloom :count_votes do
    # For each ballot, count all of the votes given to each candidate
    counts <= vote.group([:ballot_id, :candidate], count()) 
    # For each ballot, count the total number of votes cast
    counts2 <= vote.group([:ballot_id], count()) 
    # For each ballot, store the total number of votes that will be cast
    # and the total number that has been cast
    counts3 <= (ballot * counts2).pairs(:ballot_id => :ballot_id) {
      |b, c| [b.ballot_id, b.num_votes, c.num_cast]
    }

    # Find the ballots that have vote counts >= the ballot's required number
    # of votes for a "winning" candidate 
    enough <= (counts * ballot).pairs(:ballot_id => :ballot_id) { |c, b|
      [c.ballot_id, c.candidate] if c.num >= b.num_required
    } 
    
    # Add extra votes/notes data into the result
    extra <= vote.group([:ballot_id], accum(:candidate), accum(:note))
    result <= (enough * extra).pairs(:ballot_id => :ballot_id) { |w, e|
      [w.ballot_id, :success, w.vote, e.votes, e.notes]
    }

    # Failed ballots
    counts4 <= counts3.notin(enough, :ballot_id => :ballot_id)
    failed_ballots <= counts4 do |c|
      [c.ballot_id, c.num_votes, c.num_cast] if c.num_cast >= c.num_votes
    end
    
    # Failed results
    result <= (failed_ballots * extra).pairs(:ballot_id => :ballot_id) { 
      |f, e| [f.ballot_id, :fail, nil, e.votes, e.notes]
    }
  end

  # Cleanup
  bloom :cleanup do
    ballot <- (ballot * result).lefts(:ballot_id => :ballot_id)
  end
  
  # bloom :debug do
  #   stdio <~ counts.inspected
  #   stdio <~ counts2.inspected
  #   stdio <~ counts3.inspected
  #   stdio <~ counts4.inspected
  #   stdio <~ ballot.inspected
  #   stdio <~ extra.inspected
  #   stdio <~ result.inspected
  #   stdio <~ failed_ballots.inspected
  #   stdio <~ enough.inspected
  # end
end

# RatioVoteCounter is an implementation of the VoteCounterProtocol in which
# a floating point ratio is provided to specify what ratio of the total 
# no of votes is needed for a "winning" candidate. Note: the calculation is 
# rounded up, ex. votes_needed = ceil((ratio) * num_votes).
# @see RatioVoteCounter implements VoteCounterProtocol
module RatioVoteCounter
  include CountVoteCounter
  state do
    # On the client side, tell the vote counter what ratio to set. This 
    # ratio must be set before the vote starts.
    # @param [Object] ballot_id the unique id of the ballot
    # @param [Number] ratio floating point number for the percentage of 
    # votes needed for a candidate to "win"
    interface input, :ratio, [:ballot_id] => [:ratio]
  end

  # bloom :delegate do
  #   required_votes <= (ratio * ballot).pairs(:ballot_id => :ballot_id) { 
  #     |r, b| [r.ballot_id, r.ratio * b.num_votes]
  #   }
  # end
  bloom :delegate do
    required_votes <= (ratio * preballot).pairs(:ballot_id => :ballot_id) { 
      |r, b| [r.ballot_id, r.ratio * b.num_votes]
    }
  end
end

# UnanimousVoteCounter is a specific case of RatioVoteCounter, where the 
# ratio is 1.
# @see UnanimousVoteCounter extends RatioVoteCounter
module UnanimousVoteCounter
  include CountVoteCounter

  bloom :delegate do
    required_votes <= preballot do
      |b| [b.ballot_id, b.num_votes]
    end
  end
end

# MajorityVoteCounter is an implementation of the VoteCounterProtocol, where
# the no of votes needed for a majority is floor(ratio * num_members) + 1
# @see MajorityVoteCounter implements VoteCounterProtocol
module MajorityVoteCounter
  include CountVoteCounter

  bloom :delegate do
    required_votes <= preballot do
      |b| [b.ballot_id, 0.51 * b.num_votes]
    end
  end
end

# SingleVoteCounter is a module that can be composed with a 
# VoteCounterProtocol implementation to only allow a single vote from 
# each voting agent (determined by uniqueness of the ip_port field)
module SingleVoteCounter 
end

