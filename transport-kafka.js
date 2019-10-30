'use strict'

var _ = require('lodash');
var kafka = require('kafka-node');

module.exports = function kafkaTransport(pluginOptions) {

    var seneca = this
    var plugin = 'transport-kafka'
    var tu = seneca.export('transport/utils')
    var so = seneca.options();
    pluginOptions = seneca.util.deepextend(
        {
            kafka: {
                response_topic: 'transport-response',                
            }
        },
        so.transport,
        pluginOptions
    );


    seneca.add('init:kafka', init)
    seneca.add({role: 'transport', hook: 'client', type: 'kafka'}, hook_client);
    seneca.add({role: 'transport', hook: 'listen', type: 'kafka'}, hook_listen);


    function init(msg, respond) {
        respond();
    }


    function hook_listen(args, done) {

        var seneca = this;
        var type = args.type;
        var listen_options = seneca.util.clean(_.extend({}, pluginOptions[type], args));

        // initialize Listener
        const Consumer = kafka.Consumer;
        const client = new kafka.KafkaClient({kafkaHost: listen_options['kafkaHost']})
        let consumer = new Consumer(
            client,
            [{topic: args.topic, partition: 0}],
            {
                autoCommit: true,
                fetchMaxWaitMs: 1000,
                fetchMaxBytes: 1024 * 1024,
                encoding: 'utf8',
                fromOffset: false
            }
        );
        
        // on Message received
        consumer.on('message', function(message) {

            var data = tu.parseJSON(seneca, 'listen-' + 'kafka', message.value)

            tu.handle_request(seneca, data, listen_options, function (out) {
                if (out == null) return

                var response_string = tu.stringifyJSON(seneca, 'listen-' + 'kafka', out)
                
                var KafkaResponseProducer = kafka.Producer;
                var responseClient = new kafka.KafkaClient({kafkaHost: listen_options['kafkaHost']});
                var responseProducer = new KafkaResponseProducer(responseClient);      
                let response_payloads = [{topic: listen_options['response_topic'], messages: response_string}];
                responseProducer.send(response_payloads, (err, data) => {
                    if (err) {
                        console.log("TRANSPORT - ERROR SENDING RESPONSE", err);
                    }
                    if (data) {
                    }
                })            
              })
        })

        seneca.log.info('listen', 'open', listen_options, seneca);

        done();
    }


    function hook_client(args, clientdone) {

        var seneca = this;
        var type = args.type;
        var client_options = seneca.util.clean(_.extend({}, pluginOptions[type], args));

        tu.make_client(make_send, client_options, clientdone);


        // function is responsible for sending the message an receiving the response
        function make_send(spec, topic, send_done) {

            // registration of response-listener
            const KafkaResponseConsumer = kafka.Consumer;
            const responseClient = new kafka.KafkaClient({kafkaHost: client_options['kafkaHost']});
            let responseConsumer = new KafkaResponseConsumer(
                responseClient,
                [{topic: client_options['response_topic'], partition: 0}],
                {
                    autoCommit: true,
                    fetchMaxWaitMs: 1000,
                    fetchMaxBytes: 1024 * 1024,
                    encoding: 'utf8',
                    fromOffset: false
                }
            );
            seneca.log.debug('client', 'subscribe', client_options['topic'], client_options, seneca);

            // process response
            responseConsumer.on('message', function(message) {
                var input = tu.parseJSON(seneca, 'client-' + 'kafka', message.value);
                tu.handle_response(seneca, input, client_options);
            });

            // create Producer
            var Producer = kafka.Producer;
            var client = new kafka.KafkaClient({kafkaHost: client_options['kafkaHost']});
            var producer = new Producer(client);          

            // send the message
            send_done(null, function(args, done) {
                
                var outmsg = tu.prepare_request(this, args, done);
                var outstr = tu.stringifyJSON(seneca, 'client-' + 'kafka', outmsg);
                let payloads = [{topic: client_options['topic'], messages: outstr}];            

                let push_status = producer.send(payloads, (err, data) => {
                    if (err) {
                    console.log("KAFKA SEND ERROR", err);
                    }
                    if (data) {
                    }
                })            
    
            })
        }
    }

    return {
        name: plugin
    }
    
}