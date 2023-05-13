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
      if (groups[tm.topic] === undefined) {
        groups[tm.topic] = [tm.messages]
        topicNames.push(tm.topic)
        continue
      } else {
        groups[tm.topic].push(tm.messages)
      }
    }
    const topicNameLen = topicNames.length

    // now we produce one result array
    const result = Array(topicNameLen)
    for (let i = 0; i < topicNames; i++) {
      const topic = topicNames[i]

      const group = groups[topic]
      const totalLength = group.map(g => g.length).reduce((acc, cur) => acc + cur.length, 0)
      const resultsArray = Array(totalLength)

      // add each array. nested loop
      let offset = 0
      const groupLen = group.length
      for (let j = 0; j < groupLen; j++) {
        for (let k = 0; k < group[j].length; k++) {
          resultsArray[offset + j] = group[j]
        }
        offset += group[j].length
      }
      result[i] = resultsArray
    }
    return result
  },
}
