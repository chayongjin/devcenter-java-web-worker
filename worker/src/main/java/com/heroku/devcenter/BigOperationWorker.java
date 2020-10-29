package com.heroku.devcenter;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.util.ErrorHandler;

/**
 * Worker for receiving and processing BigOperations asynchronously.
 */
public class BigOperationWorker {

    public static void main(String[] args) {
        final ApplicationContext rabbitConfig = new AnnotationConfigApplicationContext(RabbitConfiguration.class);
        final ConnectionFactory rabbitConnectionFactory = rabbitConfig.getBean(ConnectionFactory.class);
        final Queue rabbitQueue = rabbitConfig.getBean(Queue.class);
        final MessageConverter messageConverter = new SimpleMessageConverter();

        // create a listener container, which is required for asynchronous message consumption.
        // AmqpTemplate cannot be used in this case
        final SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer();
        listenerContainer.setConnectionFactory(rabbitConnectionFactory);
        listenerContainer.setQueueNames(rabbitQueue.getName());

        // set the callback for message handling
        listenerContainer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                final BigOperation bigOp = (BigOperation) messageConverter.fromMessage(message);

                // simply printing out the operation, but expensive computation could happen here
                System.out.println("Received from RabbitMQ: " + bigOp);

            }
        });

        // set a simple error handler
        listenerContainer.setErrorHandler(new ErrorHandler() {
            public void handleError(Throwable t) {
                t.printStackTrace();
            }
        });

        // register a shutdown hook with the JVM
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutting down BigOperationWorker");
                listenerContainer.shutdown();
            }
        });

        // start up the listener. this will block until JVM is killed.
        listenerContainer.start();
        System.out.println("BigOperationWorker started");

        ////////////////////

        final ApplicationContext rabbitConfig2 = new AnnotationConfigApplicationContext(RabbitConfiguration2.class);
        final ConnectionFactory rabbitConnectionFactory2 = rabbitConfig2.getBean(ConnectionFactory.class);
        final Queue rabbitQueue2 = rabbitConfig2.getBean(Queue.class);
        final MessageConverter messageConverter2 = new SimpleMessageConverter();

        // create a listener container, which is required for asynchronous message consumption.
        // AmqpTemplate cannot be used in this case
        final SimpleMessageListenerContainer listenerContainer2 = new SimpleMessageListenerContainer();
        listenerContainer2.setConnectionFactory(rabbitConnectionFactory2);
        listenerContainer2.setQueueNames(rabbitQueue2.getName());

        // set the callback for message handling
        listenerContainer2.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                final BigOperation bigOp2 = (BigOperation) messageConverter2.fromMessage(message);

                // simply printing out the operation, but expensive computation could happen here
                System.out.println("Received from RabbitMQ 2 : " + bigOp2);

            }
        });

        // set a simple error handler
        listenerContainer2.setErrorHandler(new ErrorHandler() {
            public void handleError(Throwable t) {
                t.printStackTrace();
            }
        });

        // register a shutdown hook with the JVM
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutting down BigOperationWorker 2");
                listenerContainer2.shutdown();
            }
        });

        // start up the listener. this will block until JVM is killed.
        listenerContainer2.start();
        System.out.println("BigOperationWorker started 2");
    }
}
