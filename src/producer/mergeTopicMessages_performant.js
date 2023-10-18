const { KafkaJSNonRetriableError } = require('../errors')

module.exports = {
  getMergedTopicMessages(topicMessages) {
    // inspiration for faster array ops: https://dev.to/uilicious/javascript-array-push-is-945x-faster-than-array-concat-1oki
    const groups = {
      // [topicname]: [[]]
    }
    const topicNames = []

    // store each array(array)
    // loop over each, pre-allocate a results array
    // naive fill array

    for (let i = 0; i < topicMessages.length; i++) {
      const tm = topicMessages[i]
      if (!tm.topic) {
        throw new KafkaJSNonRetriableError(`Invalid topic`)
      } else if (!tm.messages) {
        throw new KafkaJSNonRetriableError(
          `Invalid messages array [${tm.messages}] for topic "${tm.topic}"`
        )
      } else if (groups[tm.topic] === undefined) {
        groups[tm.topic] = [tm.messages]
        topicNames.push(tm.topic)
      } else {
        groups[tm.topic].push(tm.messages)
      }
    }
    const topicNameLen = topicNames.length

    // now we produce one result array
    const result = Array(topicNameLen)
    for (let i = 0; i < topicNameLen; i++) {
      const topic = topicNames[i]

      const group = groups[topic]
      const totalLength = group.map(g => g.length).reduce((acc, cur) => acc + cur, 0)
      const resultsArray = Array(totalLength)

      // add each array. nested loop
      let offset = 0
      const groupLen = group.length
      for (let j = 0; j < groupLen; j++) {
        for (let k = 0; k < group[j].length; k++) {
          const msg = group[j][k]
          if (msg.value === undefined) {
            throw new KafkaJSNonRetriableError(
              `Invalid message without value for topic "${topic}": ${JSON.stringify(msg.value)}`
            )
          }
          resultsArray[offset + k] = msg
        }
        offset += group[j].length
      }
      result[i] = { topic, messages: resultsArray }
    }
    return result
  },
}
