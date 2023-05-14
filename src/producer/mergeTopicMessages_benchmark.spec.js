const old = require('./mergeTopicMessages_old')
const perf = require('./mergeTopicMessages_performant')

const getPartition = message => `partition_${Math.floor(Math.random() * 1)}`

// ffs jest, go away.
console.log = (...arg) => {
  arg.forEach(a => process.stdout.write(a))
  process.stdout.write('\n')
}

const genMessages = n =>
  Array(n)
    .fill(0)
    .map(x => ({ value: { val: Math.random() } }))

const sizes = [10, 50, 100, 500, 1000, 10000, 20000, 50000]

const ms = bi => {
  return (Number(bi) / 1000000).toFixed(2)
}

const time = async (name, values, fn) => {
  const start = process.hrtime.bigint()
  const result = await fn(values)
  const end = process.hrtime.bigint()
  return { result, time: end - start }
}

let totalPerf = 0n
let totalKafakjs = 0n

const runPerfloop = async args => {
  const { getMessages, one, two, name } = args
  console.log('\n-----------------------------')
  console.log(`Running perftest for ${name}`)
  for (const size of sizes) {
    const messages = getMessages(size)
    const kresult = await time('kafkajs', messages, one)
    const nresult = await time('new', messages, two)
    const k = kresult.time
    const n = nresult.time
    const perfImprovement = (Number(k) / Number(n)).toFixed(1)
    totalKafakjs += kresult.time
    totalPerf += nresult.time
    console.log(
      `Run for ${(size + '     ').slice(0, 5)} messages: (${ms(k)}ms) -> (${ms(
        n
      )}}ms) = ${perfImprovement}x`
    )
    // check we've actually got correct values...
    expect(kresult.result).toEqual(nresult.result)
  }
  console.log('-----------------------------')
}

async function go(fn) {
  await fn()

  const secs = bi => (Number(bi / 1000000n) / 1000).toFixed(2)

  console.log('---------------------')
  console.log('#####################')
  console.log('Final results:')
  console.log(`Total benchmark time kafkajs: ${secs(totalKafakjs)}s`)
  console.log(`Total benchmark time perf   : ${secs(totalPerf)}s`)
  const improvement = Number(totalKafakjs / totalPerf)
  console.log(`Average improvement: ${improvement.toFixed(2)}x`)
}

describe('mergeTopicMessages', () => {
  test('benchmark', async () => {
    await go(async () => {
      const randomTopic = max => `t_${Math.floor(Math.random() * max)}`
      const messages = genMessages(50000)
      const messageMax = 50000
      const messagesBlockSize = 100

      const topicBatches = [100, 200, 500, 1000, 2000]
      for (const b of topicBatches) {
        const topicMessages = Array(messageMax)
          .fill(0)
          .map(x => ({ topic: randomTopic(b), messages: messages.slice(0, messagesBlockSize) }))

        await runPerfloop({
          name: `hot-loop-2a: many topic-message pairs, t=${b}`,
          one: old.getMergedTopicMessages,
          two: perf.getMergedTopicMessages,
          getMessages: size => topicMessages.slice(0, size),
        })
      }
    })
  })
})
