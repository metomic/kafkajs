const Runner = require('./runner')

/**
 * Pool of runners that consume batches concurrently.
 *
 * @param {object} options
 * @param {import('./consumerGroup')} options.consumerGroup
 * @param {(err: Error) => Promise<void>} options.onCrash
 * @param {number} options.concurrency
 * @returns
 */
const createRunnerPool = ({
  autoCommit,
  logger: rootLogger,
  consumerGroup,
  instrumentationEmitter,
  eachBatchAutoResolve,
  eachBatch,
  eachMessage,
  heartbeatInterval,
  retry,
  onCrash,
  concurrency,
}) => {
  const logger = rootLogger.namespace('RunnerPool')
  /** @type {Runner[]} */
  let runners = []
  let running = false

  const start = async () => {
    if (running) return
    running = true

    runners = Array.from(Array(concurrency).keys()).map(
      runnerId =>
        new Runner({
          runnerId,
          autoCommit,
          logger,
          consumerGroup,
          instrumentationEmitter,
          eachBatchAutoResolve,
          eachBatch,
          eachMessage,
          heartbeatInterval,
          retry,
          onCrash,
        })
    )

    try {
      await consumerGroup.connect()
      await consumerGroup.joinAndSync()
    } catch (error) {
      onCrash(error)
      return
    }

    runners.forEach(r => r.start())
  }

  const stop = async () => {
    if (!running) return
    running = false

    await Promise.all(runners.map(r => r.stop()))
    runners = []

    try {
      await consumerGroup.leave()
    } catch (e) {}
  }

  const commitOffsets = async offsets => {
    await Promise.all(runners.map(r => r.commitOffsets(offsets)))
  }

  return { start, stop, commitOffsets }
}

module.exports = createRunnerPool
