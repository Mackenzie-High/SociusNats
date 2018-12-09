package com.mackenziehigh.socius.nats;

import com.mackenziehigh.cascade.Cascade.Stage;
import com.mackenziehigh.cascade.Cascade.Stage.Actor;
import com.mackenziehigh.cascade.Cascade.Stage.Actor.Input;
import com.mackenziehigh.cascade.Cascade.Stage.Actor.Output;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple bridge for routing messages to and/or from NATS.
 *
 * <p>
 * You can connect <i>publishers</i>, which will send messages to NATS.
 * You can connect <i>subscribers</i>, which will receive messages from NATS.
 * </p>
 *
 * <p>
 * This class does not support many of the features provided by NATS, for simplicity.
 * Of note:
 * <ul>
 * <li>Only the publish/subscribe pattern is supported. </li>
 * <li>Wildcards are <i>not</i> supported in subject names. </li>
 * <li>Queue subscriptions are not supported. </li>
 * <li>All subject names must be ASCII encoded. </li>
 * </ul>
 * </p>
 */
public final class NatsRouter
        implements AutoCloseable
{
    /**
     * This stage is used to create private actors that provide the I/O connectors.
     */
    private final Stage stage;

    /**
     * This is the connection to the NATS broker.
     */
    private final Connection connection;

    /**
     * This object will send us messages from the NATS broker.
     */
    private final Dispatcher dispatcher;

    /**
     * This map maps a NATS subject name to the corresponding publishers,
     * which will provide messages to send using that subject name.
     */
    private final ConcurrentMap<String, Publisher> publishers = new ConcurrentHashMap<>();

    /**
     * This map maps a NATS subject name to the corresponding subscribers,
     * which will receive messages with that subject name from the broker.
     */
    private final ConcurrentMap<String, Subscriber> subscribers = new ConcurrentHashMap<>();

    /**
     * This lock is used to synchronize the registration of publishers and subscribers.
     */
    private final Object lock = new Object();

    private NatsRouter (final Stage stage,
                        final Connection connection)
    {
        this.stage = Objects.requireNonNull(stage, "stage");
        this.connection = Objects.requireNonNull(connection, "connection");
        this.dispatcher = connection.createDispatcher(this::onDispatch);
    }

    /**
     * This method will execute whenever we receive a message from the broker.
     *
     * @param message was just received.
     */
    private void onDispatch (final Message message)
    {
        final String subject = message.getSubject();

        final Subscriber subscriber = subscribers.get(subject);

        if (subscriber != null)
        {
            subscriber.accept(message);
        }
    }

    /**
     * Register a publisher.
     *
     * @param connector will provide the outgoing messages.
     * @param subject will identify the outgoing messages.
     * @return this.
     */
    public NatsRouter publish (final Output<byte[]> connector,
                               final String subject)
    {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(connector, "connector");
        checkSubject(subject);

        synchronized (lock)
        {
            lookupPublisher(subject).connect(connector);
        }

        return this;
    }

    /**
     * Register a subscriber.
     *
     * @param connector will receive the incoming messages.
     * @param subject identifies the incoming messages.
     * @return this.
     */
    public NatsRouter subscribe (final Input<byte[]> connector,
                                 final String subject)
    {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(connector, "connector");
        checkSubject(subject);

        synchronized (lock)
        {
            lookupSubscriber(subject).connect(connector);
        }

        return this;
    }

    /**
     * Deregister a publisher.
     *
     * @param connector will be disconnected.
     * @param subject is needed to identify the connection.
     * @return this.
     */
    public NatsRouter unpublish (final Output<byte[]> connector,
                                 final String subject)
    {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(connector, "connector");

        synchronized (lock)
        {
            lookupPublisher(subject).disconnect(connector);
        }

        return this;
    }

    /**
     * Deregister a subscriber.
     *
     * @param connector will be disconnected.
     * @param subject is needed to identify the connection.
     * @return this.
     */
    public NatsRouter unsubscribe (final Input<byte[]> connector,
                                   final String subject)
    {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(connector, "connector");

        synchronized (lock)
        {
            lookupSubscriber(subject).disconnect(connector);
        }

        return this;
    }

    /**
     * Factory Method.
     *
     * @param stage will be used to create private actors.
     * @param connection will be used to connect to the NATS broker.
     * @return the new router.
     */
    public static NatsRouter newNatsRouter (final Stage stage,
                                            final Connection connection)
    {
        return new NatsRouter(stage, connection);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close ()
            throws Exception
    {
        connection.closeDispatcher(dispatcher);
        connection.close();
    }

    private void createPublisherIfNeeded (final String subject)
    {
        if (publishers.containsKey(subject) == false)
        {
            publishers.put(subject, new Publisher(subject));
        }
    }

    private Publisher lookupPublisher (final String subject)
    {
        createPublisherIfNeeded(subject);
        return publishers.get(subject);
    }

    private void createSubscriberIfNeeded (final String subject)
    {
        if (subscribers.containsKey(subject) == false)
        {
            subscribers.put(subject, new Subscriber(subject));
        }
    }

    private Subscriber lookupSubscriber (final String subject)
    {
        createSubscriberIfNeeded(subject);
        return subscribers.get(subject);
    }

    private void checkSubject (final String subject)
    {
        final String ascii = new String(subject.getBytes(StandardCharsets.US_ASCII));

        if (ascii.matches("\\p{Alnum}+(\\.\\p{Alnum}+)*") == false)
        {
            throw new IllegalArgumentException("Bad Subject: " + subject);
        }
    }

    /**
     * Encapsulates all of the publisher connections for a single subject.
     */
    private final class Publisher
    {
        /**
         * NATS Subject Name.
         */
        private final String subject;

        /**
         * This actor will received messages from publishers
         * and then forward them to the NATS broker.
         */
        private final Actor<byte[], byte[]> actor;

        /**
         * These are the connections to the publishers.
         */
        private final Set<Input<byte[]>> registrations = Collections.newSetFromMap(new ConcurrentHashMap<>());

        public Publisher (final String subject)
        {
            this.subject = subject;
            this.actor = stage.newActor().withScript(this::onMessage).create();
        }

        private void onMessage (final byte[] message)
        {
            connection.publish(subject, message);
        }

        public void connect (final Output<byte[]> connector)
        {
            actor.input().connect(connector);
        }

        public void disconnect (final Output<byte[]> connector)
        {
            /**
             * Disconnect the publisher, so we stop receiving messages to forward.
             */
            actor.input().disconnect(connector);
            registrations.remove(connector);

            /**
             * If that was the last publisher to disconnect,
             * then go ahead and destroy this object too.
             */
            if (registrations.isEmpty())
            {
                publishers.remove(this);
            }
        }
    }

    /**
     * Encapsulates all of the subscriber connections for a single subject.
     */
    private final class Subscriber
    {
        /**
         * NATS Subject Name.
         */
        private final String subject;

        /**
         * Provides the fan-out connection to the subscribers.
         */
        private final Actor<byte[], byte[]> actor;

        /**
         * These are the connections to the publishers.
         */
        private final Set<Input<byte[]>> registrations = Collections.newSetFromMap(new ConcurrentHashMap<>());

        public Subscriber (final String subject)
        {
            this.subject = subject;
            this.actor = stage.newActor().withScript((byte[] x) -> x).create();
        }

        public void accept (final Message message)
        {
            actor.accept(message.getData());
        }

        public void connect (final Input<byte[]> connector)
        {
            actor.output().connect(connector);
            dispatcher.subscribe(subject);
        }

        public void disconnect (final Input<byte[]> connector)
        {
            /**
             * Disconnect the subscriber itself.
             */
            actor.output().disconnect(connector);
            registrations.remove(connector);

            /**
             * If that was the last subscriber to disconnect,
             * then go ahead and destroy this object too.
             */
            if (registrations.isEmpty())
            {
                dispatcher.unsubscribe(subject);
                publishers.remove(this);
            }
        }
    }

//    public static void main (String[] args)
//            throws Exception
//    {
//        final ScheduledExecutorService clock = Executors.newSingleThreadScheduledExecutor();
//
//        final Stage stage = Cascade.newStage();
//        final Connection conn = Nats.connect();
//
//        final Actor<byte[], byte[]> actor0 = stage.newActor().withScript((byte[] x) -> x).create();
//        final Actor<byte[], byte[]> actor1 = stage.newActor().withScript((byte[] x) -> System.out.println("X = " + new String(x))).create();
//        final Actor<byte[], byte[]> actor2 = stage.newActor().withScript((byte[] x) -> System.out.println("Y = " + new String(x))).create();
//
//        final NatsRouter router = NatsRouter.newNatsRouter(stage, conn);
//
//        router.publish("ny.timesq", actor0.output());
//        router.subscribe("ny.timesq", actor1.input());
//        router.subscribe("ny.timesq", actor2.input());
//
//        clock.scheduleAtFixedRate(() -> actor0.input().send(Instant.now().toString().getBytes()), 0, 10, TimeUnit.MILLISECONDS);
//
//        Thread.sleep(100_000);
//    }
}