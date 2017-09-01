package org.apache.activemq.artemis.protocol.amqp.sasl;

public interface ClientSASL {

   String getName();
   byte[] getInitialResponse();
   byte[] getResponse(byte[] challenge);
}
