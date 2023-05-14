const createSendMessages = require('./sendMessages')
const { KafkaJSError, KafkaJSNonRetriableError } = require('../errors')
const { CONNECTION_STATUS } = require('../network/connectionStatus')
const { getMergedTopicMessages } = require('./mergeTopicMessages_performant')

module.exports = ({
  logger,
  cluster,
  partitioner,
  eosManager,
  idempotent,
  retrier,
  getConnectionStatus,
}) => {
  const sendMessages = createSendMessages({
    logger,
    cluster,
    retrier,
    partitioner,
    eosManager,
  })

  const validateConnectionStatus = () => {
    const connectionStatus = getConnectionStatus()

    switch (connectionStatus) {
      case CONNECTION_STATUS.DISCONNECTING:
        throw new KafkaJSNonRetriableError(
          `The producer is disconnecting; therefore, it can't safely accept messages anymore`
        )
      case CONNECTION_STATUS.DISCONNECTED:
        throw new KafkaJSError('The producer is disconnected')
    }
  }

  /**
   * @typedef {Object} TopicMessages
   * @property {string} topic
   * @property {Array} messages An array of objects with "key" and "value", example:
   *                         [{ key: 'my-key', value: 'my-value'}]
   *
   * @typedef {Object} SendBatchRequest
   * @property {Array<TopicMessages>} topicMessages
   * @property {number} [acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   *
   * @property {number} [timeout=30000] The time to await a response in ms
   * @property {Compression.Types} [compression=Compression.Types.None] Compression codec
   *
   * @param {SendBatchRequest}
   * @returns {Promise}
   */
  const sendBatch = async ({ acks = -1, timeout, compression, topicMessages = [] }) => {
    if (idempotent && acks !== -1) {
      throw new KafkaJSNonRetriableError(
        `Not requiring ack for all messages invalidates the idempotent producer's EoS guarantees`
      )
    }
    const mergedTopicMessages = getMergedTopicMessages(topicMessages)
    validateConnectionStatus()

    return await sendMessages({
      acks,
      timeout,
      compression,
      topicMessages: mergedTopicMessages,
    })
  }

  /**
   * @param {ProduceRequest} ProduceRequest
   * @returns {Promise}
   *
   * @typedef {Object} ProduceRequest
   * @property {string} topic
   * @property {Array} messages An array of objects with "key" and "value", example:
   *                         [{ key: 'my-key', value: 'my-value'}]
   * @property {number} [acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   * @property {number} [timeout=30000] The time to await a response in ms
   * @property {Compression.Types} [compression=Compression.Types.None] Compression codec
   */
  const send = async ({ acks, timeout, compression, topic, messages }) => {
    const topicMessage = { topic, messages }
    return sendBatch({
      acks,
      timeout,
      compression,
      topicMessages: [topicMessage],
    })
  }

  return {
    send,
    sendBatch,
  }
}
