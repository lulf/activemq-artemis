/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.proton.handler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.security.auth.Subject;

import org.apache.activemq.artemis.protocol.amqp.proton.ProtonInitializable;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.ServerSASL;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.jboss.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class ProtonHandler extends ProtonInitializable {

   private static final Logger log = Logger.getLogger(ProtonHandler.class);

   private static final byte SASL = 0x03;

   private static final byte BARE = 0x00;

   private final Transport transport = Proton.transport();

   private final Connection connection = Proton.connection();

   private final Collector collector = Proton.collector();
   private final boolean isServer;

   private List<EventHandler> handlers = new ArrayList<>();

   private Sasl sasl;

   private ClientSASL saslClientMechanism;

   private final ReentrantLock lock = new ReentrantLock();

   private final long creationTime;

   private Map<String, ServerSASL> saslHandlers;

   private SASLResult saslResult;

   protected volatile boolean dataReceived;

   protected boolean receivedFirstPacket = false;

   private final Executor flushExecutor;

   protected final ReadyListener readyListener;

   boolean inDispatch = false;

   public ProtonHandler(Executor flushExecutor, final boolean isServer) {
      this.isServer = isServer;
      this.flushExecutor = flushExecutor;
      this.readyListener = () -> flushExecutor.execute(() -> {
         flush();
      });
      this.creationTime = System.currentTimeMillis();
      transport.bind(connection);
      connection.collect(collector);
   }

   public long tick(boolean firstTick) {
      lock.lock();
      try {
         if (!firstTick) {
            try {
               if (connection.getLocalState() != EndpointState.CLOSED) {
                  long rescheduleAt = transport.tick(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
                  if (transport.isClosed()) {
                     throw new IllegalStateException("Channel was inactive for to long");
                  }
                  return rescheduleAt;
               }
            } catch (Exception e) {
               log.warn(e.getMessage(), e);
               transport.close();
               connection.setCondition(new ErrorCondition());
            }
            return 0;
         }
         return transport.tick(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
      } finally {
         lock.unlock();
         flushBytes();
      }
   }

   public int capacity() {
      lock.lock();
      try {
         return transport.capacity();
      } finally {
         lock.unlock();
      }
   }

   public void lock() {
      lock.lock();
   }

   public void unlock() {
      lock.unlock();
   }

   public boolean tryLock(long time, TimeUnit timeUnit) {
      try {
         return lock.tryLock(time, timeUnit);
      } catch (InterruptedException e) {

         Thread.currentThread().interrupt();
         return false;
      }
   }

   public Transport getTransport() {
      return transport;
   }

   public Connection getConnection() {
      return connection;
   }

   public ProtonHandler addEventHandler(EventHandler handler) {
      handlers.add(handler);
      return this;
   }

   public void createServerSASL(ServerSASL[] handlers) {
      this.sasl = transport.sasl();
      saslHandlers = new HashMap<>();
      String[] names = new String[handlers.length];
      int count = 0;
      for (ServerSASL handler : handlers) {
         saslHandlers.put(handler.getName(), handler);
         names[count++] = handler.getName();
      }
      this.sasl.server();
      sasl.setMechanisms(names);
   }

   public void flushBytes() {

      for (EventHandler handler : handlers) {
         if (!handler.flowControl(readyListener)) {
            return;
         }
      }

      lock.lock();
      try {
         while (true) {
            int pending = transport.pending();

            if (pending <= 0) {
               break;
            }

            // We allocated a Pooled Direct Buffer, that will be sent down the stream
            ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(pending);
            ByteBuffer head = transport.head();
            buffer.writeBytes(head);

            for (EventHandler handler : handlers) {
               handler.pushBytes(buffer);
            }

            transport.pop(pending);
         }
      } finally {
         lock.unlock();
      }
   }

   public SASLResult getSASLResult() {
      return saslResult;
   }

   public void inputBuffer(ByteBuf buffer) {
      dataReceived = true;
      lock.lock();
      try {
         while (buffer.readableBytes() > 0) {
            int capacity = transport.capacity();

            if (!receivedFirstPacket) {
               try {
                  byte auth = buffer.getByte(4);
                  if (auth == SASL || auth == BARE) {
                     if(!isServer) {
                        dispatchAuth(auth == SASL);
                     }
                     /*
                     * there is a chance that if SASL Handshake has been carried out that the capacity may change.
                     * */
                     capacity = transport.capacity();
                  }
               } catch (Throwable e) {
                  log.warn(e.getMessage(), e);
               }

               receivedFirstPacket = true;
            }

            if (capacity > 0) {
               ByteBuffer tail = transport.tail();
               int min = Math.min(capacity, buffer.readableBytes());
               tail.limit(min);
               buffer.readBytes(tail);

               flush();
            } else {
               if (capacity == 0) {
                  log.debugf("abandoning: readableBytes=%d", buffer.readableBytes());
               } else {
                  log.debugf("transport closed, discarding: readableBytes=%d, capacity=%d", buffer.readableBytes(), transport.capacity());
               }
               break;
            }
         }
      } finally {
         lock.unlock();
      }
   }

   public boolean checkDataReceived() {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   public long getCreationTime() {
      return creationTime;
   }

   public void flush() {
      lock.lock();
      try {
         transport.process();
         checkSASL();
      } finally {
         lock.unlock();
      }

      dispatch();
   }

   public void close(ErrorCondition errorCondition) {
      lock.lock();
      try {
         if (errorCondition != null) {
            connection.setCondition(errorCondition);
         }
         connection.close();
      } finally {
         lock.unlock();
      }

      flush();
   }

   protected void checkSASL() {
      if (isServer) {
         if (sasl != null && sasl.getRemoteMechanisms().length > 0) {
            ServerSASL mechanism = saslHandlers.get(sasl.getRemoteMechanisms()[0]);
            if (mechanism != null) {

               byte[] dataSASL = new byte[sasl.pending()];
               sasl.recv(dataSASL, 0, dataSASL.length);

               if (log.isTraceEnabled()) {
                  log.trace("Working on sasl::" + (dataSASL != null && dataSASL.length > 0 ? ByteUtil.bytesToHex(dataSASL, 2) : "Anonymous"));
               }

               saslResult = mechanism.processSASL(dataSASL);

               if (saslResult != null && saslResult.isSuccess()) {
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
                  saslHandlers.clear();
                  saslHandlers = null;
               } else {
                  sasl.done(Sasl.SaslOutcome.PN_SASL_AUTH);
               }
               sasl = null;
            } else {
               // no auth available, system error
               sasl.done(Sasl.SaslOutcome.PN_SASL_SYS);
            }
         }

      } else {
         if (sasl != null) {
            switch (sasl.getState()) {
               case PN_SASL_IDLE:
                  if (sasl.getRemoteMechanisms().length != 0) {
                     dispatchMechanismsOffered(sasl.getRemoteMechanisms());

                     if (saslClientMechanism== null) {
                        // TODO - log unknown mechanisms
                        dispatchAuthFailed();
                     } else {
                        sasl.setMechanisms(saslClientMechanism.getName());
                        byte[] initialResponse = saslClientMechanism.getInitialResponse();
                        if (initialResponse != null) {
                           sasl.send(initialResponse, 0, initialResponse.length);
                        }
                     }
                  }
                  break;
               case PN_SASL_STEP:
                  int challengeSize = sasl.pending();
                  byte[] challenge = new byte[challengeSize];
                  sasl.recv(challenge, 0, challengeSize);
                  byte[] response = saslClientMechanism.getResponse(challenge);
                  sasl.send(response, 0, response.length);
                  break;
               case PN_SASL_FAIL:
                  // TODO - log error
                  dispatchAuthFailed();
                  sasl = null;
                  break;
               case PN_SASL_PASS:
                  // TODO - log success
                  saslResult = new SASLResult()
                  {
                     @Override
                     public String getUser()
                     {
                        return null;
                     }
                     
                     @Override
                     public boolean isSuccess()
                     {
                        return true;
                     }
                  };

                  sasl = null;
                  break;
               case PN_SASL_CONF:
                  // do nothing
                  break;
            }
         }
      }
   }

   private void dispatchAuthFailed() {
      for (EventHandler h : handlers) {
         h.onAuthFailed(this, getConnection());
      }
   }

   private void dispatchMechanismsOffered(final String[] mechs) {
      for (EventHandler h : handlers) {
         h.onSaslMechanismsOffered(this, mechs);
      }
   }

   private void dispatchAuth(boolean sasl) {
      for (EventHandler h : handlers) {
         h.onAuthInit(this, getConnection(), sasl);
      }
   }

   private void dispatch() {
      Event ev;

      lock.lock();
      try {
         if (inDispatch) {
            // Avoid recursion from events
            return;
         }
         try {
            inDispatch = true;
            while ((ev = collector.peek()) != null) {
               for (EventHandler h : handlers) {
                  if (log.isTraceEnabled()) {
                     log.trace("Handling " + ev + " towards " + h);
                  }
                  try {
                     Events.dispatch(ev, h);
                  } catch (Exception e) {
                     log.warn(e.getMessage(), e);
                     ErrorCondition error = new ErrorCondition();
                     error.setCondition(AmqpError.INTERNAL_ERROR);
                     error.setDescription("Unrecoverable error: " +
                        (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
                     connection.setCondition(error);
                     connection.close();
                  }
               }

               collector.pop();
            }

         } finally {
            inDispatch = false;
         }
      } finally {
         lock.unlock();
      }

      flushBytes();
   }

   public void open(String containerId, Map<Symbol, Object> connectionProperties) {
      this.transport.open();
      this.connection.setContainer(containerId);
      this.connection.setProperties(connectionProperties);
      this.connection.open();
      flush();
   }

   public void setClientMechanism(final ClientSASL saslClientMech) {
      this.saslClientMechanism = saslClientMech;
   }
   public void createClientSASL()
   {
      this.sasl = transport.sasl();
      this.sasl.client();
   }
}
