package com.mackenziehigh.socius.nats;

import com.mackenziehigh.cascade.Cascade;
import com.mackenziehigh.cascade.Cascade.Stage;
import com.mackenziehigh.cascade.Cascade.Stage.Actor;
import io.nats.client.Connection;
import static io.nats.client.Connection.Status.CLOSED;
import static io.nats.client.Connection.Status.CONNECTED;
import io.nats.client.Nats;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit Test.
 */
public final class NatsRouterTest
{
    /**
     * Object Under Test.
     */
    private NatsRouter router;

    /**
     * This is the connection to the NATS broker.
     */
    private Connection connection;

    /**
     * Shared Stage.
     */
    private final Stage stage = Cascade.newStage();

    /**
     * Messages received by Subscriber #1.
     */
    private final BlockingQueue<byte[]> queue1 = new LinkedBlockingQueue<>();

    /**
     * Messages received by Subscriber #2.
     */
    private final BlockingQueue<byte[]> queue2 = new LinkedBlockingQueue<>();

    /**
     * Publisher #1.
     */
    private final Actor<byte[], byte[]> actor1 = stage.newActor().withScript((byte[] x) -> x).create();

    /**
     * Publisher #2.
     */
    private final Actor<byte[], byte[]> actor2 = stage.newActor().withScript((byte[] x) -> x).create();

    /**
     * Subscriber #1.
     */
    private final Actor<byte[], Boolean> actor3 = stage.newActor().withScript((byte[] x) -> queue1.add(x)).create();

    /**
     * Subscriber #2.
     */
    private final Actor<byte[], Boolean> actor4 = stage.newActor().withScript((byte[] x) -> queue2.add(x)).create();

    @Before
    public void setup ()
            throws IOException,
                   InterruptedException
    {
        connection = Nats.connect();
        router = NatsRouter.newNatsRouter(stage, connection);

        assertEquals(CONNECTED, connection.getStatus());
    }

    @After
    public void destroy ()
            throws Exception
    {
        router.close();
        stage.close();

        /**
         * The close() method closes the connection to the broker.
         */
        assertEquals(CLOSED, connection.getStatus());
    }

    /**
     * Test: 20181209002245614458
     *
     * <p>
     * Case: Basic Throughput.
     * </p>
     *
     * @throws java.lang.InterruptedException
     */
    @Test
    public void test20181209002245614458 ()
            throws InterruptedException
    {
        final String subject = UUID.randomUUID().toString().replace("-", ".");

        router.publish(actor1.output(), subject);
        router.publish(actor2.output(), subject);
        router.subscribe(actor3.input(), subject);
        router.subscribe(actor4.input(), subject);

        actor1.input().send("A".getBytes());
        assertArrayEquals("A".getBytes(), queue1.take());
        assertArrayEquals("A".getBytes(), queue2.take());

        actor1.input().send("B".getBytes());
        assertArrayEquals("B".getBytes(), queue1.take());
        assertArrayEquals("B".getBytes(), queue2.take());

        actor2.input().send("C".getBytes());
        assertArrayEquals("C".getBytes(), queue1.take());
        assertArrayEquals("C".getBytes(), queue2.take());

        actor2.input().send("D".getBytes());
        assertArrayEquals("D".getBytes(), queue1.take());
        assertArrayEquals("D".getBytes(), queue2.take());
    }

    /**
     * Test: 20181209002245614565
     *
     * <p>
     * Case: No messages are received after subscriber unsubscribes.
     * </p>
     *
     * @throws java.lang.InterruptedException
     */
    @Test
    public void test20181209002245614565 ()
            throws InterruptedException
    {
        final String subject = UUID.randomUUID().toString().replace("-", ".");

        router.publish(actor1.output(), subject);
        router.subscribe(actor3.input(), subject);

        /**
         * Send and receive a message.
         */
        actor1.input().send("A".getBytes());
        assertArrayEquals("A".getBytes(), queue1.take());

        /**
         * Send a message, but do not receive,
         * because the subscriber unsubscribed.
         */
        router.unsubscribe(actor3.input(), subject);
        actor1.input().send("B".getBytes());
        assertNull(queue1.poll(200, TimeUnit.MILLISECONDS));

        /**
         * Send and receive a message,
         * because the subscriber re-subscribed.
         */
        router.subscribe(actor3.input(), subject);
        actor1.input().send("C".getBytes());
        assertArrayEquals("C".getBytes(), queue1.take());
    }

    /**
     * Test: 20181209002245614595
     *
     * <p>
     * Case: No messages are sent after publisher unsubscribes.
     * </p>
     *
     * @throws java.lang.InterruptedException
     */
    @Test
    public void test20181209002245614595 ()
            throws InterruptedException
    {
        final String subject = UUID.randomUUID().toString().replace("-", ".");

        router.publish(actor1.output(), subject);
        router.subscribe(actor3.input(), subject);

        /**
         * Send and receive a message.
         */
        actor1.input().send("A".getBytes());
        assertArrayEquals("A".getBytes(), queue1.take());

        /**
         * Send a message, but do not receive,
         * because the publisher unsubscribed.
         */
        router.unpublish(actor1.output(), subject);
        actor1.input().send("B".getBytes());
        assertNull(queue1.poll(200, TimeUnit.MILLISECONDS));

        /**
         * Send and receive a message,
         * because the publisher re-subscribed.
         */
        router.publish(actor1.output(), subject);
        actor1.input().send("C".getBytes());
        assertArrayEquals("C".getBytes(), queue1.take());
    }

    /**
     * Test: 20181209002245614622
     *
     * <p>
     * Case: If multiple publishers are registered for a given subject,
     * then removing one publisher will not affect the others.
     * </p>
     *
     * @throws java.lang.InterruptedException
     */
    @Test
    public void test20181209002245614622 ()
            throws InterruptedException
    {
        final String subject = UUID.randomUUID().toString().replace("-", ".");

        router.publish(actor1.output(), subject);
        router.publish(actor2.output(), subject);
        router.subscribe(actor3.input(), subject);

        /**
         * Send a message using each publisher.
         */
        actor1.input().send("A".getBytes());
        assertArrayEquals("A".getBytes(), queue1.take());
        actor2.input().send("B".getBytes());
        assertArrayEquals("B".getBytes(), queue1.take());

        /**
         * Disconnect one of the publishers.
         */
        router.unpublish(actor1.output(), subject);

        /**
         * Send a message using each publisher.
         */
        actor1.input().send("C".getBytes());
        assertNull(queue1.poll(200, TimeUnit.MILLISECONDS));
        actor2.input().send("D".getBytes());
        assertArrayEquals("D".getBytes(), queue1.take());

        /**
         * Reconnect the publisher.
         */
        router.publish(actor1.output(), subject);

        /**
         * Send a message using each publisher.
         */
        actor1.input().send("E".getBytes());
        assertArrayEquals("E".getBytes(), queue1.take());
        actor2.input().send("F".getBytes());
        assertArrayEquals("F".getBytes(), queue1.take());
    }

    /**
     * Test: 20181209002245614648
     *
     * <p>
     * Case: If multiple subscribers are registered for a given subject,
     * then removing one publisher will not affect the others.
     * </p>
     *
     * @throws java.lang.InterruptedException
     */
    @Test
    public void test20181209002245614648 ()
            throws InterruptedException
    {
        final String subject = UUID.randomUUID().toString().replace("-", ".");

        router.publish(actor1.output(), subject);
        router.subscribe(actor3.input(), subject);
        router.subscribe(actor4.input(), subject);

        /**
         * Receive a message using each subscriber.
         */
        actor1.input().send("A".getBytes());
        assertArrayEquals("A".getBytes(), queue1.take());
        assertArrayEquals("A".getBytes(), queue2.take());
        actor1.input().send("B".getBytes());
        assertArrayEquals("B".getBytes(), queue1.take());
        assertArrayEquals("B".getBytes(), queue2.take());

        /**
         * Disconnect one of the subscribers.
         */
        router.unsubscribe(actor3.input(), subject);

        /**
         * Try to receive a message using each subscriber.
         * The one that was disconnected should not receive anything.
         */
        actor1.input().send("C".getBytes());
        assertNull(queue1.poll(200, TimeUnit.MILLISECONDS));
        assertArrayEquals("C".getBytes(), queue2.take());

        /**
         * Reconnect the subscriber.
         */
        router.subscribe(actor3.input(), subject);

        /**
         * Try to receive a message using each subscriber.
         * Both subscribers should get the message.
         */
        actor1.input().send("C".getBytes());
        assertArrayEquals("C".getBytes(), queue1.take());
        assertArrayEquals("C".getBytes(), queue2.take());
    }

    /**
     * Test: 20181209015854981796
     *
     * <p>
     * Case: Messages are only routed to interested subscribers.
     * </p>
     *
     * @throws java.lang.InterruptedException
     */
    @Test
    public void test20181209015854981796 ()
            throws InterruptedException
    {
        final String subject1 = UUID.randomUUID().toString().replace("-", ".");
        final String subject2 = UUID.randomUUID().toString().replace("-", ".");

        router.publish(actor1.output(), subject1);
        router.publish(actor2.output(), subject2);
        router.subscribe(actor3.input(), subject1);
        router.subscribe(actor4.input(), subject2);

        /**
         * Send messages using each publisher/subscriber pair.
         */
        for (int i = 0; i < 100; i++)
        {
            actor1.input().send("A".getBytes());
            actor2.input().send("B".getBytes());
            assertArrayEquals("A".getBytes(), queue1.take());
            assertArrayEquals("B".getBytes(), queue2.take());
        }
    }

    /**
     * Test: 20181209005228640286
     *
     * <p>
     * Case: Subject names must be alpha-numeric.
     * </p>
     */
    @Test (expected = IllegalArgumentException.class)
    public void test20181209005228640286 ()
    {
        router.publish(actor1.output(), "$3"); // "$" is illegal.
    }
}
