require('source-map-support').install()

import * as d from './'

const op = d.op({
  schema: {
    KeySchema: [
      {
        AttributeName: 'temp',
        KeyType: 'HASH'
      }
    ]
  }
})

const hot = op.query()
  .where('ridiculousnessOfGoodLookingness').gte(8)

const adult = op.query()
  .where('age').gte(18)

adult.where('temp').between(['hot', 'cold'])

const jen = op.query()
  .where('firstName').eq('Jennifer')

jen.where('hotness').between([1, 10])

const hotOrJen = d.combine.or([hot, jen])
debugger
const builder = d.combine.and([hotOrJen, adult])

// builder
//   .keyConditions
//     .where('c').lte(7)
//     .or()
//     .where('d').eq(8)
//     .or()
//     .where('e').lte(2)
//     .where('e').gte(1)

// console.log(JSON.stringify(builder.getParams(), null, 2))

// const builder1 = createQueryBuilder()
// builder1
//   .where('a').eq(1)
//   .where('b').in([2, 3])
//   .keyConditions
//     .where('c').lte(7)
//     .or()
//       .where('d').eq(8)

// builder1
//   .or()
//     .where('e').lte(2)
//     .where('e').gte(1)

// console.log(JSON.stringify(hot.getParams(), null, 2))
// console.log(JSON.stringify(jen.getParams(), null, 2))
// console.log(JSON.stringify(hotOrJen.getParams(), null, 2))
// console.log(JSON.stringify(adult.getParams(), null, 2))

console.log(JSON.stringify(builder.getParams(), null, 2))
