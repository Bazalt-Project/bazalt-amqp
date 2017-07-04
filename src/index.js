'use strict';

// requirements
var amqp = require('amqplib');

// Load local requirements
var Queue = require('./queue.js');

// Load connection
function connect(self, callback) {
    // Connect to the amqp server
    self.$__connection = amqp.connect(self.$__uri);
    self.$__channel    = self.$__connection.then(function(connection) {

        // Callback if connected
        if('function' === typeof callback)
        {
            setTimeout(callback);
        }

        // Switch for confirmation or not
        if(true === self.$__confirm)
        {
            return connection.createConfirmChannel();
        }

        return connection.createChannel();
    });

    self.$__connected = true;
}

// Amqp Class
class Amqp {

    constructor(uri, confirm = true) {
        // Store needed values
        this.$__uri        = uri;
        this.$__confirm    = confirm;
        this.$__connected  = false;
        this.$__connection = false;
        this.$__queues     = [];
    }

    connect(callback) {
        if(true === this.$__connected)
        {
            throw new Error('Amqp is already connected!');
        }

        // Connect to the Amqp server
        connect(this, callback);

        return this;
    }

    reconnect(callback) {
        // Connect to the Amqp server
        connect(this, callback);

        // Reload each queues
        for(let queue of this.$__queues)
        {
            queue.reload();
        }
    }

    queue(name, options) {
        // If the queue is already created, reuse it
        if(true === this.$__queues[name] instanceof Queue)
        {
            return this.$__queues[name];
        }

        // Create the queue
        var queue = new Queue(this.$__channel, name, options);

        // Add the queue to the list of queues
        this.$__queues[name] = queue;

        return queue;
    }

    on(event, callback) {
        // Pass event handling
        this.$__connection.then(function(connection) {
            connection.on(event, callback);
        });
    }

    close() {
        if(
            false === this.$__connected ||
            false === this.$__connection
        ) {
            throw new Error('Amqp is not connected!');
        }

        this.$__connection.then(function(connection) {

            // Close the connection
            connection.close();
        });
    }
}

// Export Amqp
module.exports = Amqp;
