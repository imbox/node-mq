'use strict'
const BufferedQueue = require('buffered-queue')
const R = require('ramda')
const RoutingKeyParser = require('rabbit-routingkey-parser')
const util = require('util')
const parser = new RoutingKeyParser()

module.exports = Mq

function Mq (opts) {
  if (!new.target) {
    return new Mq(opts)
  }

  this.rabbot = opts.rabbot || require('rabbot')
  this.topology = R.clone(opts.topology)
  this.connectionName = this.topology.connection.name || 'default'
  this.unhandledTimeout = opts.unhandledTimeout || 120 * 1000
  this.statisticsEnabled = opts.statisticsEnabled || false

  const logger = (this.logger = opts.logger || {
    debug: console.log,
    warn: console.error
  })

  const serviceName = opts.serviceName || 'default'
  const rabbot = this.rabbot
  const connectionName = this.connectionName

  // Set always used connection values
  this.topology.connection.noCacheKeys = true
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
    const publish = this.publish.bind(this)
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
  const rabbot = this.rabbot
  const logger = this.logger
  const connectionName = this.connectionName
  const unhandledTimeout = this.unhandledTimeout
  const queue = opts.queue
  const types = opts.types || ['#']
  const handler = opts.handler
  const onUncaughtException = opts.onUncaughtException
  const statisticsEnabled = this.statisticsEnabled
  const statsQueue = this.statsQueue

  const onMessage = R.curry((type, message) => {
    const startDateTimeMs = new Date().getTime()
    const startTime = now()

    // Warn if message hasn't been handled after `unhandledTimeout`
    message.timeoutHandler = setTimeout(() => {
      logger.warn(`TimeoutError: ${util.inspect(message)}`)
      message.reject()
      message.ack = () => {}
      message.nack = () => {}
      message.reply = () => {}
      message.reject = () => {}
    }, unhandledTimeout)

    const ack = message.ack
    message.ack = () => {
      if (statisticsEnabled) {
        statsQueue.add({
          routingKey: message.fields.routingKey,
          queue: queue,
          startTime: startDateTimeMs,
          json: JSON.stringify(message.body),
          duration: now() - startTime
        })
      }
      clearTimeout(message.timeoutHandler)
      return ack(arguments)
    }

    const nack = message.nack
    message.nack = () => {
      clearTimeout(message.timeoutHandler)
      return nack(arguments)
    }

    const reject = message.reject
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
  const self = this
  return this.topology.queues.map(q => {
    return self.rabbot.stopSubscription(q.name, self.connectionName)
  })
}

Mq.prototype.shutdown = function () {
  return this.rabbot.shutdown()
}

Mq.prototype.publish = function (exchangeName, opts) {
  return this.rabbot.publish(exchangeName, opts, this.connectionName)
}

Mq.prototype.request = function (exchangeName, opts) {
  return this.rabbot.request(exchangeName, opts, this.connectionName)
}

const getNanoSeconds = () => {
  const hr = process.hrtime()
  return hr[0] * 1e9 + hr[1]
}
const loadTime = getNanoSeconds()
const now = () => (getNanoSeconds() - loadTime) / 1e6
