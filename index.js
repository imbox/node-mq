'use strict'
let BufferedQueue = require('buffered-queue')
let R = require('ramda')
let RoutingKeyParser = require('rabbit-routingkey-parser')
let util = require('util')

let parser = new RoutingKeyParser()

module.exports = Mq

function Mq (opts) {
  if (!new.target) {
    return new Mq(opts)
  }

  this.rabbot = opts.rabbot
  this.topology = R.clone(opts.topology)
  this.connectionName = this.topology.connection.name || 'default'
  this.unhandledTimeout = opts.unhandledTimeout || 120 * 1000
  this.statisticsEnabled = opts.statisticsEnabled || false

  let logger = this.logger = opts.logger || {
    debug: console.log,
    warn: console.error
  }

  let serviceName = opts.serviceName || 'default'
  let rabbot = this.rabbot
  let connectionName = this.connectionName

  rabbot.setAckInterval(20)

  rabbot.on(`${connectionName}.connection.opened`, () => {
    logger.debug('Rabbitmq connection opened')
  })

  rabbot.on(`${connectionName}.connection.closed`, () => {
    logger.debug('Rabbitmq connection closed (intentional)')
  })

  rabbot.on(`${connectionName}.connection.failed`, () => {
    logger.warn('Rabbitmq connection failed (unintentional)')
  })

  rabbot.on(`${connectionName}.connection.configured`, () => {
    logger.debug('Rabbitmq connection configured')
  })

  rabbot.on(`${connectionName}.connection.unreachable`, () => {
    throw new Error('Rabbitmq connection unreachable')
  })

  if (this.statisticsEnabled) {
    let publish = this.publish.bind(this)
    this.statsQueue = new BufferedQueue('stats-queue', {
      size: 1000,
      flushTimeout: 5000,
      verbose: false,
      customResultFunction (items) {
        publish('stats-exchange', {
          routingKey: 'stats.v1.messagesProcessTime',
          body: {
            type: 'messagesProcessTime',
            service: serviceName,
            data: items,
            timestamp: new Date().toISOString()
          }
        })
      }
    })
  }
}

Mq.prototype.configure = function () {
  return this.rabbot.configure(this.topology)
}

Mq.prototype.handle = function (opts) {
  let rabbot = this.rabbot
  let logger = this.logger
  let connectionName = this.connectionName
  let unhandledTimeout = this.unhandledTimeout
  let queue = opts.queue
  let types = opts.types || ['#']
  let handler = opts.handler
  let onUncaughtException = opts.onUncaughtException
  let statisticsEnabled = this.statisticsEnabled
  let statsQueue = this.statsQueue

  let onMessage = R.curry((type, message) => {
    let startDateTimeMs = new Date().getTime()
    let startTime = now()

    // Warn if message hasn't been handled after `unhandledTimeout`
    message.timeoutHandler = setTimeout(() => {
      logger.warn(`TimeoutError: ${util.inspect(message)}`)
      message.reject()
    }, unhandledTimeout)

    let ack = message.ack
    message.ack = () => {
      if (statisticsEnabled) {
        statsQueue.add({
          routingKey: message.fields.routingKey,
          queue: queue,
          startTime: startDateTimeMs,
          duration: now() - startTime
        })
      }
      clearTimeout(message.timeoutHandler)
      return ack(arguments)
    }

    let nack = message.nack
    message.nack = () => {
      clearTimeout(message.timeoutHandler)
      return nack(arguments)
    }

    let reject = message.reject
    message.reject = () => {
      clearTimeout(message.timeoutHandler)
      return reject(arguments)
    }

    // Parse routing key
    message.fields.parts = parser.parse(type, message.fields.routingKey)

    // Remove the raw buffer content
    delete message.content

    return handler(message)
  })

  return types.map(function (type) {
    let handle = rabbot.handle({
      connectionName: connectionName,
      queue: queue,
      type: type,
      handler: onMessage(type),
      autoNack: !onUncaughtException
    })

    if (onUncaughtException) {
      handle.catch((err, msg) => {
        clearTimeout(msg.timeoutHandler)
        onUncaughtException(err, msg)
      })
    }
    return handle
  })
}

Mq.prototype.close = function () {
  return this.rabbot.close(this.connectionName)
}

Mq.prototype.unsubscribe = function () {
  let self = this
  return this.topology.queues.map((q) => {
    return self.rabbot.stopSubscription(q.name, self.connectionName)
  })
}

Mq.prototype.shutdown = function () {
  return this.rabbot.shutdown()
}

Mq.prototype.publish = function (exchangeName, opts) {
  return this.rabbot.publish(exchangeName, opts, this.connectionName)
}

let getNanoSeconds = () => {
  let hr = process.hrtime()
  return hr[0] * 1e9 + hr[1]
}
let loadTime = getNanoSeconds()
let now = () => ((getNanoSeconds() - loadTime) / 1e6)
