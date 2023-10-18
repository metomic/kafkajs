const old = require('./mergeTopicMessages_old')
const perf = require('./mergeTopicMessages_performant')

describe('Producer > performantMergeTopicMessages', () => {
  test('group messages per partition', () => {
    const tm = Array(10)
      .fill(0)
      .map((_, i) => ({
        topic: `topic_${i}`,
        messages: new Array(10).fill(0).map((_, i) => ({
          key: `key-${i}`,
          value: `value-${i}`,
          headers: {
            [`header-a${i}`]: `header-value-a${i}`,
            [`header-b${i}`]: `header-value-b${i}`,
            [`header-c${i}`]: `header-value-c${i}`,
          },
        })),
      }))

    const resultOld = old.getMergedTopicMessages(tm)
    const resultPerf = perf.getMergedTopicMessages(tm)
    expect(resultOld).toEqual(resultPerf)
  })
})
