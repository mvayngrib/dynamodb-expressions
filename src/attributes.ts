import {
  ExpressionAttributeNameMap,
  ExpressionAttributeValueMap
} from 'aws-sdk/clients/dynamodb'

export class ExpressionAttributes {
  public names: ExpressionAttributeNameMap
  public values: ExpressionAttributeValueMap
  private counter:number
  constructor() {
    this.names = {}
    this.values = {}
    this.counter = 0
  }

  public addName = (name) => {
    const keys = name.split('.');
    const path = `#${keys.join('.#').replace(/[^\w\.#]/g, '')}`;
    keys.forEach(key => {
      this.names[`#${key.replace(/[^\w\.]/g, '')}`] = key
    })

    return path
  }

  public addValue = (value) => {
    const name = ':v' + this.counter++
    this.values[name] = value
    return name
  }
}



// internals.buildInFilterExpression = (key, existingValueNames, values) => {
//   const path = `#${key}`;

//   const attributeNames = {};
//   attributeNames[path.split('.')[0]] = key.split('.')[0];

//   const attributeValues = _.reduce(values, (result, val) => {
//     const existing = _.keys(result).concat(existingValueNames);
//     const p = internals.uniqAttributeValueName(key, existing);
//     result[p] = internals.formatAttributeValue(val);
//     return result;
//   }, {});

//   return {
//     attributeNames: attributeNames,
//     statement: `${path} IN (${_.keys(attributeValues)})`,
//     attributeValues: attributeValues
//   };
// };
