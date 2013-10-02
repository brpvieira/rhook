redis = require('redis')
EventEmitter2 = require('eventemitter2').EventEmitter2
_ = require('underscore')
A = require('async')

class Hook extends EventEmitter2
    constructor: (config) ->
        defaults =
            channel: "hook"
            wildcard: true
            delimiter: "::"
            auth: false
            host: null
            port: null
            redisOptions:
                retry_max_delay: 1000

        @config = _.defaults(config,defaults)
        @config.newListener = false

        for handler in _.functions(@)
            do (handler) =>
                @[handler] = _.bind(@[handler], @) if handler.indexOf('_on') == 0

        @_watchForReadyState = _.bind(@_watchForReadyState, @)

        Object.defineProperty(@, "_publisherReady", {
            get: () =>
                return false unless @_publisher
                return @_publisher.connected
        })


        Object.defineProperty(@, "_subscriberReady", {
            get: () =>
                return false unless @_subscriber && @_subscriber.connected
                return @_subscriber.subscription_set["sub #{@config.channel}"]
        })

        super(_.pick(@config, 'wildcard', 'delimiter', 'newListener'))

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

    _watchForReadyState: () ->
        return if @_isWatching
        @_isWatching = true
        doNothing = (cb) -> setTimeout(cb, 100)
        testFn = () -> @_publisherReady and @_subscriberReady
        doneFn = () ->
            @_isWatching = false
            @_emit("hook#{@config.delimiter}ready")
        
        A.until(_.bind(testFn,@), doNothing, _.bind(doneFn, @))

    start: () ->
        @_watchForReadyState()
        @_initPublisher()
        @_initSubscriber()

    

    #
    # Subscriber event handlers
    #
    _onSubscribe: (channel, count) ->
        @_emit('subscribe', channel, count)

    _onUnsubscribe: (channel, count) ->
        @_emit('unsubscribe'. channel, count)

    _onSubscriberReady: () ->
        @_subscriber.subscribe(@config.channel)

    _onMessage: (channel, msg) ->
        return false if channel != @config.channel
        {source, version, name, data} = JSON.parse(msg)

        return false if source == @config.name
        @_emit("#{source}#{@config.delimiter}#{name}", data)

    _onSubscriberEnd: () ->
        @_emit('subscriberend')
            
    _onSubscriberError: (err) ->
        @_emit('subscribererror', err)
        @_onConnectionError(err)

    #
    # Publisher event handlers
    #
    _onPublisherReady: () ->
        @_emit('publisherready')
    
        
    _onPublisherEnd: () ->
        @_emit('publisherend')

    _onPublisherError: (err) ->
        @_emit('publishererror', err)
        @_onConnectionError(err)

    #
    # General error handler
    #
    _onConnectionError: (err) ->
        @_watchForReadyState()
        retryCount = Math.max(@_subscriber.attempts, @_publisher.attempts)
        retryDelay = Math.max(@_subscriber.retry_delay, @_publisher.retry_delay)
        @_emit('connectionerror', err, retryCount, retryDelay)
    
    _bindSubscriberEvents: () ->
        @_subscriber.on('message', @_onMessage)
        @_subscriber.on('subscribe', @_onSubscribe)
        @_subscriber.on('unsubscribe', @_onUnsubscribe)
        @_subscriber.on('ready', @_onSubscriberReady)
        @_subscriber.on('end', @_onSubscriberEnd)
        @_subscriber.on('error', @_onSubscriberError)

    _bindPublisherEvents: () ->
        @_publisher.on('ready', @_onPublisherReady)
        @_publisher.on('end', @_onPublisherEnd)
        @_publisher.on('error', @_onPublisherError)

    _emit: () ->
        EventEmitter2.prototype.emit.apply(@, arguments)
    
    _initPublisher: () ->
        return @_publisher if @_publisherReady

        @_publisher = redis.createClient(@config.port, @config.host,@config.redisOptions)
        @_bindPublisherEvents()
        @_publisher.auth(@config.auth) if @config.auth
        return @_publisher

    _initSubscriber: () ->
        return @_subscriber if @_subscriberReady

        @_subscriber = redis.createClient(@config.port, @config.host,@config.redisOptions)
        @_bindSubscriberEvents()
        @_subscriber.auth(@config.auth) if @config.auth

        return @_subscriber

    _resetClients: () ->
        @_publisherReady = false
        @_subscriberReady = false



module.exports = { Hook }


