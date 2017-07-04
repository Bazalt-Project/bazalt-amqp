'use strict';

// Private method
function loadQueue(self, promise, name, length, options = { durable: false }) {

    // Store needed values
    self.$__name    = name;
    self.$__promise = promise;
    self.$__options = options;
    self.$__length  = length;

    // Assert that the queue exist
    self.$__promise.then(function(channel) {

        // Check for the existence of the Queue
        channel.assertQueue(name, options);

        self.$__queue = channel;

        // Set the prefetch back
        if(null !== length)
        {
            self.prefetch(length);
        }

        // Set the consumer back
        if('function' === typeof self.$__consumer)
        {
            self.consume(self.$__consumer, self.$__consumerOptions);
        }
    });
}

// Queue Class
class Queue {

    constructor(promise, name, options = { durable: false }) {
        
        // Load the queue with  values
        loadQueue(this, promise, name, null, options);
    }

    reload() {

        // Reload the queue with previous values
        loadQueue(this, this.$__promise, this.$__name, this.$__length, this.$__options);
    }

    prefetch(length) {
        // Save the prefetch length
        this.$__length = length;

        // Prefetch the channel
        this.$__queue = this.$__queue.then(function(channel) {
            channel.prefetch(length);
        });
    }

    send(object, options) {
        // Extract the name of the queue
        var name = this.$__name;

        // Convert the object to message
        var message = new Buffer(JSON.stringify(object));

        // Send it
        this.$__queue.sendToQueue(name, message, options);
    }

    consume(callback, options = { noAck: false }) {
        if('function' !== typeof callback)
        {
            throw new Error('There is no valid Callback to consume messages!');
        }

        // Save informations about consumer
        this.$__consumer        = callback;
        this.$__consumerOptions = options;

        // If queue not ready, previous information will replay the process
        if(!this.$__queue)
        {
            return;
        }

        // Extract the name of the queue
        var self = this,
            name = this.$__name;

        // Consume published messages
        this.$__queue.consume(name, function(message) {
            // Function to accept the message
            var accept = function() {

                // Acknowledge the message
                if(false === options.noAck) {
                    self.$__queue.ack(message);
                }
            };

            // Function to reject the message
            var reject = function() {

                // Acknowledge the message
                if(false === options.noAck) {
                    self.$__queue.nack(message);
                }
            };

            try {
                // Convert the message
                var object = JSON.parse(message.content.toString());

                // Call the callback with a check function
                callback(null, object, accept, reject);
            } catch (error) {
                // Send back the err
                callback(error, null, accept, reject);
            }
        });
    }
}

// Export Queue
module.exports = Queue;