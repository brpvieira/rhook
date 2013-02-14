redis = require('redis')
EventEmitter2 = require('eventemitter2').EventEmitter2
_ = require('underscore')
async = require('async')

class Hook extends EventEmitter2
    constructor: (config) ->
        defaults =
            channel: "hook"
            wildcard: true
            delimiter: "::"
            auth: false
            host: null
            port: null
            redisOptions: null

        @config = _.defaults(config,defaults)
        @config.newListener = false
        super(_.pick(@config, 'wildcard', 'delimiter', 'newListener'))

    _bindSubscriberEvents: () ->
        @_subscriber.on('subscribe', (channel, count) =>
            @_subscriber.on('message', (channel, msg) =>
                @_onMessage(channel, msg)
            )
            @_emit('subscribe', channel, count)
        )

        @_subscriber.on('unsubscribe', (channel, count) =>
            @_emit('unsubscribe'. channel, count)
        )

        @_subscriber.on('ready', () =>
            @_subscriber.subscribe(@config.channel)
        )

        @_subscriber.on('end', () =>
            @_emit('subscriberend')
        )


        @_subscriber.on('error', (err) ->
            @_emit('subscribererror', err)
        )

    _bindPublisherEvents: () ->
        @_publisher.on('ready', () =>
            @_emit('publisherready')
        )

        @_publisher.on('end', () =>
            @_emit('publisherend')
        )

        @_publisher.on('error', (err) =>
            @_publisherReady = false
            @_emit('publishererror', err)
        )

    _emit: () ->
        EventEmitter2.prototype.emit.apply(@, arguments)

    emit: (name, data) ->
        return @_emit(arguments...) if name == "newListener"
        @_initPublisher() if !@_publisherReady

        eventData = JSON.stringify({
            source: @config.name
            version: @config.version || "0.0.0"
            name: name
            data: data or {}
        })
        @_publisher.publish(@config.channel, eventData)

    _onMessage: (channel, msg) ->
        return false if channel != @config.channel
        {source, version, name, data} = JSON.parse(msg)

        return false if source == @config.name
        @_emit("#{source}#{@config.delimiter}#{name}", data)

    _initPublisher: (cb) ->
        return cb(null, @_publisher) if @_publisherReady

        @_publisher = redis.createClient(@config.port, @config.host,@config.redisOptions)
        @_publisher.auth(@config.auth) if @config.auth
        @_bindPublisherEvents()

        errorHandler = (err) =>
            @_publisherReady = false
            @off('publisherready', successHandler)
            cb(err)

        successHandler = () =>
            @_publisherReady = true
            @off('publishererror', errorHandler)
            cb(null, @_publisher)

        @once('publishererror', errorHandler)
        @once('publisherready', successHandler)

    _initSubscriber: (cb) ->
        return cb(null, @_subscriber) if @_subscriberReady

        @_subscriber = redis.createClient(@config.port, @config.host,@config.redisOptions)
        @_subscriber.auth(@config.auth) if @config.auth
        @_bindSubscriberEvents()

        errorHandler = (err) =>
            @_subscriberReady = false
            @off('subscribe', successHandler)
            cb(err)

        successHandler = () =>
            @_subscriberReady = true
            @off('subscriberrerror', errorHandler)
            cb(null, @_subscriber)

        @once('subscriberrerror', errorHandler)
        @once('subscribe', successHandler)

    _resetClientsAndEmitConnectionEnd: () ->
        @_publisherReady = false
        @_subscriberReady = false
        @_publisher = null
        @_subscriber = null
        @_emit("hook#{@config.delimiter}connection#{@config.delimiter}end")

    _onSubscriberEnd: () ->
        @off('publisherend', @_onPubliserEnd)
        @_publisher.end()
        @_resetClientsAndEmitConnectionEnd()

    _onPubliserEnd: () ->
        @off('subscriberend', @_onSubscriberEnd)
        @_subscriber.end()
        @_resetClientsAndEmitConnectionEnd()

    start: () ->
        async.parallel([
            (cb) => @_initPublisher(cb)
            (cb) => @_initSubscriber(cb)
        ],(err, results) =>
            return @_emit('error', err) if err?
            @_emit("hook#{@config.delimiter}ready")
            @once('publisherend', () => @_onPubliserEnd())
            @once('subscriberend', () => @_onSubscriberEnd())
        )

module.exports = { Hook }