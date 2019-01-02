'use strict'

const amqp = require('amqplib')
const console = require('console')

let connection, channel, queue

/**
 * 'host'      => 'rabbitmq',
 * 'port'      => 5672,
 * 'username'  => 'guest',
 * 'password'  => 'guest',
 * 'namespace' => '/',
 * 'insist'    => true,
 * 'project'   => 'puzzle',
 */

class Event {
	constructor() {
		this.project = 'puzzle'
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

	async init(config) {
		if (!this.isInit) {
			if (config['host']) {
				config.hostname = config['host']
				delete config['host']
			}
			if (config['namespace']) {
				config.vhost = config['namespace']
				delete config['namespace']
			}
			if (config['project']) {
				this.project = config['project']
				delete config['project']
			}
			this.config = Object.assign({}, this.config, config)

			connection = await amqp.connect(this.config)
			channel = await connection.createChannel()
			await channel.assertExchange(this.project, 'topic', {
				durable: true
			})
		}

		if (config['debug']) {
			this.debug = true
			this.debugInfo('Debug Mode On')
			this.debugInfo('Settings:', this.config)
		}

		return this
	}

	async emit(event, message, options) {
		this.debugInfo('Emit Event:', event, 'with message:', message)
		if (typeof args !== 'string') {
			message = JSON.stringify(message)
		}
		channel.publish(this.project, event, new Buffer(message), {
			persistent: true
		})
	}

	/**
	 * bind event
	 * @param {String} routingKey 
	 * @param {Function} eventListener 
	 */
	async on(routingKey, eventListener, options) {
		let queueName = `${this.project}-${routingKey}`
		queue = await channel.assertQueue(queueName, {
			exclusive: false
		})
		channel.bindQueue(queue.queue, this.project, routingKey)
		this.debugInfo('Binding:', routingKey, 'using queue:', queueName)
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
		}, {
			noAck: true
		})
	}
}

module.exports = new Event()