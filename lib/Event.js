'use strict'

const amqp = require('amqplib')
const console = require('console')

let connection, channel, queue

/**
 * 'host'      => 'rabbitmq',
 * 'port'      => 5672,
 * 'username'  => 'guest',
 * 'password'  => 'guest',
 * 'namespace' => 'puzzle.services',
 * 'insist'    => true,
 */

class Event {
	constructor() {
		this.exchange = 'core.events'
		this.namespace = 'global'
		this.config = {
			protocol: 'amqp',
			hostname: 'localhost',
			port: 5672,
			username: 'guest',
			password: 'guest',
			vhost: '/',
		}
		this.isInit = false
		this.debug = false
	}

	debugInfo(...args) {
		if (this.debug) {
			args.unshift('Event:')
			console.log(...args)
		}
	}

	async init(config = {}) {
		if (!this.isInit) {
			if (config['host']) {
				config.hostname = config['host']
				delete config['host']
			}
			this.config = Object.assign({}, this.config, config)
			if (config['namespace']) {
				this.namespace = config['namespace']
				delete config['namespace']
			}

			connection = await amqp.connect(this.config)
			channel = await connection.createChannel()
			await channel.assertExchange(this.exchange, 'topic', this.config['exchangeOptions'] || {})
		}

		if (config['debug']) {
			this.debug = true
			this.debugInfo('Debug Mode On')
			this.debugInfo('Settings:', this.config)
		}

		return this
	}

	async emit(event, message, namespace = null, messageOptions = null) {
		event = (namespace ? namespace : this.namespace) + '.' + event
		this.debugInfo('Emit Event:', event, 'with message:', message)
		if (typeof args !== 'string') {
			message = JSON.stringify(message)
		}
		channel.publish(this.exchange, event, new Buffer(message), messageOptions || {})
	}

	/**
	 * bind event
	 * @param {String} namespace
	 * @param {String} event 
	 * @param {Function} eventListener 
	 * @param {Object} queueOptions 
	 */
	async on(namespace, event, eventListener, queueOptions = null, consumeOptions = null) {
		let queueName = `${namespace}-${event}`
		event = namespace + '.' + event
		queue = await channel.assertQueue(queueName, queueOptions || {})
		channel.bindQueue(queue.queue, this.exchange, event)
		this.debugInfo('Binding:', event, 'using queue:', queueName)
		channel.consume(queue.queue, (message) => {
			this.debugInfo('Receive Message:', message)
			let routingKey = message.fields.routingKey
			let messageContent = message.content.toString()
			/**
			 * @param {String} messageContent
			 * @param {String} routingKey
			 * @param {Object} message {content: Buffer, fields: {deliveryTag:'', consumerTag: '', exchange: '', routingKey: '', redelivered: false }, properties: Object}
			 */
			eventListener(messageContent, routingKey, message)
		}, consumeOptions || {})
	}
}

module.exports = new Event()