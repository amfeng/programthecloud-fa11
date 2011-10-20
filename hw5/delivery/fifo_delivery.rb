require 'rubygems'
require 'bud'
require 'delivery/delivery'

# Note that this provides at-least-once semantics. If you need exactly-once, the
# receiver-side can record the message IDs that have been received to avoid
# processing duplicate messages.
module FIFODelivery
  include DeliveryProtocol

  state do
    # This is used by the receiver to mantain a buffer of messages that have been sent to it
    table :buffer, pipe_in.schema
    # This is a table that contains the name of the sender and the message ID that should be seen by the receiver next
    table :client_current_state, [:src] => [:current_message_id]
    # This is a channel used by the client to send messages to the server
    channel :pipe_talk, [:@dst, :src, :ident] => [:payload]
    # This is a scratch used by the server to store it's messages in FIFO order
    scratch :pipe_chan, pipe_in.schema
  end

  # The client sends all the messages it receives to the server
  bloom :send do
    pipe_talk <~ pipe_in
  end

  bloom :receive do
    # Save all the messages the server receives in the buffer
    buffer <= pipe_talk {|p| [p.dst, p.src, p.ident, p.payload]}
    
    # If the server has not seen a message from that client before, then add that
    # client to the client_current_state table with an expected message ID of 0
    client_current_state <+ pipe_talk do |p|
      [p.src, 0] unless client_current_state{|n| n.src}.include? p.src
    end

    # If the message has an ID that matches with the next expected message ID from that client
    # add it to the pipe_chan scratch
    pipe_chan <+ (client_current_state * buffer).rights(:src => :src,
                                                        :current_message_id => :ident)

    # stdio <~ pipe_talk {|p| ["at #{budtime}, pipe_talk has #{p.inspect}"]}
    # stdio <~ pipe_chan {|p| ["at #{budtime}, pipe_chan has #{p.inspect}"]}
    
    # Update the client_current_state to show that the next message expected from that client is 
    # one more than the message ID received now
    client_current_state <- (client_current_state * buffer).lefts(:src => :src, 
                                                                  :current_message_id => :ident)
    client_current_state <+ (client_current_state * buffer).lefts(:src => :src, 
                                                                  :current_message_id => :ident) do 
      |s| [s.src,  s.current_message_id + 1] 
    end
    
    # Messages that have made their way across successfully, put them in pipe_sent
    pipe_sent <= pipe_chan
    
    # Take the successful messages out of the buffer
    buffer <- pipe_chan
  end
end
