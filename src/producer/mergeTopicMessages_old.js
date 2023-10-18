const { KafkaJSNonRetriableError } = require('../errors')

module.exports = {
  getMergedTopicMessages(topicMessages) {
    if (topicMessages.some(({ topic }) => !topic)) {
      throw new KafkaJSNonRetriableError(`Invalid topic`)
    }

    for (const { topic, messages } of topicMessages) {
      if (!messages) {
        throw new KafkaJSNonRetriableError(
          `Invalid messages array [${messages}] for topic "${topic}"`
        )
      }

      const messageWithoutValue = messages.find(message => message.value === undefined)
      if (messageWithoutValue) {
        throw new KafkaJSNonRetriableError(
          `Invalid message without value for topic "${topic}": ${JSON.stringify(
            messageWithoutValue
          )}`
        )
      }
    }

    const mergedTopicMessages = topicMessages.reduce((merged, { topic, messages }) => {
      const index = merged.findIndex(({ topic: mergedTopic }) => topic === mergedTopic)

      if (index === -1) {
        merged.push({ topic, messages })
      } else {
        merged[index].messages = [...merged[index].messages, ...messages]
      }

      return merged
    }, [])
    return mergedTopicMessages
  },
}
