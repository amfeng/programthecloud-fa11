require 'rubygems'
require 'bud'
require 'membership/membership'

module VoteCounting
  state do
    interface input, :incomingRows, [:reqid] => [:key, :value]
    interface input, :numberRequired, [:reqid] => [:requiredNumberOfElements]
    interface output, :result, [:reqid] => [:value]
  end

  bloom do
    
  end
