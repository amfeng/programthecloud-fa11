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
end
