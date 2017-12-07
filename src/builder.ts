import AWS = require('aws-sdk')
import pick = require('object.pick')
import {
  ExpressionAttributes
} from './attributes'

import {
  ExpressionAttributeNameMap,
  ExpressionAttributeValueMap
} from 'aws-sdk/clients/dynamodb'

const COPY_PARAMS = [
  'ScanIndexForward',
  'IndexName',
  'ConsistentRead',
  'ReturnConsumedCapacity',
  'Limit',
  'AttributesToGet',
  'Select',
  'ExclusiveStartKey'
]

const functionOperators = [
  'attribute_exists',
  'attribute_not_exists',
  'attribute_type',
  'begins_with',
  'contains',
  'NOT contains',
  'size'
]

const isFunctionOperator = (operator:string) => functionOperators.includes(operator)
const keyOperators = [
  'equals',
  'eq',
  'lte',
  'lt',
  'gte',
  'gt',
  'beginsWith',
  'between',
]

const queryFilterOperators = [
  'equals',
  'eq',
  'ne',
  'lte',
  'lt',
  'gte',
  'gt',
  'null',
  'notNull',
  'contains',
  'notContains',
  'in',
  'beginsWith',
  'between',
]

const scanFilterOperators = [
  'equals',
  'eq',
  'ne',
  'lte',
  'lt',
  'gte',
  'gt',
  'null',
  'notNull',
  'contains',
  'notContains',
  'in',
  'beginsWith',
  'between',
]

const conditionToOperator = {
  equals: '=',
  eq: '=',
  ne: '<>',
  lte: '<=',
  lt: '<',
  gte: '>=',
  gt: '>',
  null: 'attribute_not_exists',
  notNull: 'attribute_exists',
  exists: 'attribute_exists',
  contains: 'contains',
  notContains: 'NOT contains',
  in: 'IN',
  beginsWith: 'begins_with',
  between: 'BETWEEN'
}

export const types = [
  'query',
  'scan',
  'update'
]

type AttributeMap = {
  [key:string]: any
}

type SetWhereCondition = (...any) => IBuilder

interface IWhereBuilder {
  [operator:string]: SetWhereCondition
}

interface IBuilder {
  type: string
  schema: AWS.DynamoDB.Types.TableDescription
  parent?: IBuilder
  allowedConditions: AttributeMap
  attributes: ExpressionAttributes
  filterStatements: string[]
  keyStatements: string[]
  where:(key:string) => IWhereBuilder
  usingIndex:Function
  index:string
  getFilterExpression:() => string
  getKeyExpression?:() => string
  getNames:() => ExpressionAttributeNameMap
  getValues:() => ExpressionAttributeValueMap
  getParams:() => any
  or?: (builder?:IBuilder) => IBuilder
}

const typeToExpression = {
  query: 'FilterExpression',
  scan: 'FilterExpression',
  update: 'UpdateExpression',
  condition: 'ConditionExpression',
}

const getAllowedOperators = ({ schema, type, attribute, index }) => {
  if (type === 'scan') {
    return scanFilterOperators
  }

  if (type === 'query') {
    const { KeySchema } = index
      ? schema.GlobalSecondaryIndexes.find(({ IndexName }) => IndexName === index)
      : schema

    const isKeyAttribute = KeySchema
      .find(({ AttributeName }) => AttributeName === attribute)

    return isKeyAttribute ? keyOperators : queryFilterOperators
  }

  throw new Error('not supported')
}

const typeToOperators = {
  ['query']: queryFilterOperators,
  ['scan']: scanFilterOperators,
  // ['update']: 'UpdateExpression',
  // ['condition']: 'ConditionExpression',
}

export const createWhereBuilder = (builder:IBuilder) => {
  return (attribute:string):IWhereBuilder => {
    const { attributes, schema, type, index } = builder
    const operators = getAllowedOperators({ schema, type, attribute, index })
    const statements = operators === keyOperators ? builder.keyStatements : builder.filterStatements
    const wb:IWhereBuilder = {}
    operators.forEach(operator => {
      wb[operator] = (value) => {
        const statement = getStatement(attributes, conditionToOperator[operator], attribute, value)
        statements.push(statement)
        return builder
      }
    })

    return wb
  }
}

export const createBuilder = ({
  parent,
  schema,
  attributes=new ExpressionAttributes(),
  type
}: {
  parent?:IBuilder,
  schema: AWS.DynamoDB.Types.TableDescription,
  attributes?:ExpressionAttributes,
  type: string
}):IBuilder => {
  if (!(typeof type === 'number' || type)) {
    throw new Error('"type" is required')
  }

  if (!attributes) attributes = new ExpressionAttributes()

  const filterStatements:string[] = []
  let keyStatements:string[]
  if (type === 'query') keyStatements = []

  const expressionName = typeToExpression[type]
  const baseParams = {
    TableName: schema.TableName
  }

  let setConditions
  const builder = {
    type,
    schema,
    parent,
    get index() {
      return baseParams.IndexName
    },
    attributes,
    filterStatements,
    keyStatements,
    usingIndex: (indexName) => {
      if (setConditions) {
        throw new Error('usingIndex() must be called before where()')
      }

      const match = schema.GlobalSecondaryIndexes.find(({ IndexName }) => IndexName === indexName)
      if (!match) {
        throw new Error(`no index found with IndexName ${indexName}`)
      }

      baseParams.IndexName = indexName
      return builder
    },
    ascending: () => {
      baseParams.ScanIndexForward = true
      return builder
    },
    descending: () => {
      baseParams.ScanIndexForward = false
      return builder
    },
    limit: (value:number) => {
      baseParams.Limit = value
      return builder
    },
    consistentRead: (value:boolean) => {
      baseParams.ConsistentRead = value
      return builder
    },
    startKey: (value:any) => {
      baseParams.ExclusiveStartKey = value
      return builder
    },
    get where() {
      setConditions = true
      return createWhereBuilder(builder)
    },
    getParams: () => {
      const params = {
        ...baseParams,
        ExpressionAttributeNames: builder.getNames(),
        ExpressionAttributeValues: builder.getValues()
      }

      const filterExpression = builder.getFilterExpression(),
      if (filterExpression) {
        params[expressionName] = filterExpression
      }

      if (builder.getKeyExpression) {
        const exp = builder.getKeyExpression()
        if (exp) {
          params.KeyConditionExpression = builder.getKeyExpression()
        }
      }

      return params
    },
    getFilterExpression: () => filterStatements.join(' AND '),
    getNames: () => attributes.names,
    getValues: () => attributes.values
  }

  if (keyStatements) {
    builder.getKeyExpression = () => keyStatements.join(' AND ')
  }

  return builder
}

const createBuilderFromBuilder = (builder:IBuilder):IBuilder => {
  const { schema, attributes, type, parent } = builder
  return createBuilder({
    schema,
    attributes,
    type,
    parent
  })
}

const combineBuilders = (combinator:string, builders:IBuilder[]) => {
  if (combinator === 'OR' && builders.some(b => b.keyStatements.length)) {
    throw new Error(`can't use OR for key conditions`)
  }

  const builder = createBuilderFromBuilder(builders[0])
  const { type, attributes } = builder
  const mkGetExpression = (method) => () => {
    let parts = builders
      .map(builder => builder[method]())
      .filter(expression => expression)

    if (parts.length > 1) {
      parts = parts.map(expression => `(${expression})`)
    }

    return parts.join(` ${combinator} `)
  }

  builder.getFilterExpression = mkGetExpression('getFilterExpression')
  if (combinator === 'AND') {
    builder.getKeyExpression = mkGetExpression('getKeyExpression')
  }

  builder.getParams = () => {
    const params = {
      ExpressionAttributeNames: attributes.names,
      ExpressionAttributeValues: attributes.values
    }

    const filterExpression = builder.getFilterExpression()
    if (filterExpression) {
      params[typeToExpression[type]] = filterExpression
    }

    const keyExpression = builder.getKeyExpression && builder.getKeyExpression()
    if (keyExpression) {
      params.KeyConditionExpression = keyExpression
    }

    const copyParams = builders.map(builder => pick(builder.getParams(), COPY_PARAMS))
    Object.assign(params, ...copyParams)
    return params
  }

  return builder
}

export const combine = {
  and: combineBuilders.bind(null, 'AND'),
  or: combineBuilders.bind(null, 'OR')
}

export const op = ({ schema }) => {
  const attributes = new ExpressionAttributes()
  return ['query', 'scan'].reduce((builders, type) => {
    builders[type] = (...opts) => {
      return createBuilder({
        type,
        attributes,
        schema,
        ...opts
      })
    }

    return builders
  }, {})
}

const getStatement = (
  attributes:ExpressionAttributes,
  operator:string,
  key:string,
  value?:any
) => {
  const attName = attributes.addName(key)
  const valNames = [].concat(value).map(v => attributes.addValue(v))
  if (operator === 'IN') {
    return `${attName} IN (${valNames.join(', ')})`
  }

  if (operator === 'attribute_exists' && value === false) {
    operator = 'attribute_not_exists'
    value = null
  } else if (operator === 'attribute_exists' && value === true) {
    value = null
  }

  if (isFunctionOperator(operator)) {
    if (value != null) {
      return `${operator}(${attName}, ${valNames[0]})`
    }

    return `${operator}(${attName})`
  }

  if (operator === 'BETWEEN') {
    return `${attName} BETWEEN ${valNames[0]} AND ${valNames[1]}`
  }

  return [attName, operator, valNames[0]].join(' ')
}
