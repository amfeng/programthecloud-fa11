require 'rubygems'
require 'bud'
require 'membership/membership'

module VoteCounting
  state do
    interface input, :incomingRows, [:reqid] => [:key, :value]
    interface input, :numberRequired, [:requiredNumberOfElements]
    interface output, :result, [:value]
  end

  bloom do
    
  end
