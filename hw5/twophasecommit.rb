require 'rubygems'
require 'bud'

module TwoPCCoordinator
  state do
    interface input, :commit_request, [:reqid] => []
    interface output, :commit_response, [:reqid] => [:status]
  end
end

module AgreementConfig
  state do
    interface input :add_participant, [:ident] => [:host]
    interface input :delete_participant, [:reqid, :partid]
    interface input :pause_participant, [:reqid, :partid]
    interface input :resume_participant, [:reqid, :partid]
    interface output :ack, [:reqid]
  end

  bloom :route do
    # Broadcast commit_request to all the participants in the member table

    # If all the participants send a "Yes to commit" ack back - send a "commit"
    # request to all the participants

    # Once all the participants send back a "commited" ack, then the coordinator
    # can put a commit message in the commit_response output interface
  end
end
