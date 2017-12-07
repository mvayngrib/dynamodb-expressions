
// copied from dynogels, promisified

export const batchProcess = async ({
  client,
  builder,
  worker
}) => {
  let lastEvaluatedKey = null
  let retry = true
  let keepGoing = true
  let response
  const params = builder.getParams()
  while (keepGoing && (lastEvaluatedKey || retry)) {
    try {
      response = await client[builder.type](params).promise()
    } catch (err) {
      if (err.retryable) {
        retry = true
        await waitImmediate()
        continue
      }

      retry = false
      throw err
    }

    retry = false
    lastEvaluatedKey = response.LastEvaluatedKey
    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey
    } else {
      delete params.ExclusiveStartKey
    }

    keepGoing = await worker(response)
  }
}

export const loadAll = async ({
  builder,
  client
}) => {
  const responses = []
  await batchProcess({
    builder,
    client,
    worker: batch => {
      responses.push(batch)
    }
  })

  return mergeResults(responses, builder.schema.TableName)
}

export const mergeResults = (responses, tableName) => {
  const result = {
    Items: [],
    ConsumedCapacity: {
      CapacityUnits: 0,
      TableName: tableName
    },
    Count: 0,
    ScannedCount: 0
  }

  const merged = responses.reduce((memo, resp) => {
    if (!resp) {
      return memo
    }

    memo.Count += resp.Count || 0
    memo.ScannedCount += resp.ScannedCount || 0

    if (resp.ConsumedCapacity) {
      memo.ConsumedCapacity.CapacityUnits += resp.ConsumedCapacity.CapacityUnits || 0
    }

    if (resp.Items) {
      memo.Items = memo.Items.concat(resp.Items)
    }

    if (resp.LastEvaluatedKey) {
      memo.LastEvaluatedKey = resp.LastEvaluatedKey
    }

    return memo
  }, result)

  if (merged.ConsumedCapacity.CapacityUnits === 0) {
    delete merged.ConsumedCapacity
  }

  if (merged.ScannedCount === 0) {
    delete merged.ScannedCount
  }

  return merged
}

const waitImmediate = () => new Promise(resolve => setImmediate(resolve))
