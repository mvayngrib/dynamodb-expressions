"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
require('source-map-support').install();
const d = require("./");
const op = d.op({
    schema: {
        KeySchema: [
            {
                AttributeName: 'temp',
                KeyType: 'HASH'
            }
        ]
    }
});
const hot = op.query()
    .where('ridiculousnessOfGoodLookingness').gte(8);
const adult = op.query()
    .where('age').gte(18);
adult.where('temp').between(['hot', 'cold']);
const jen = op.query()
    .where('firstName').eq('Jennifer');
jen.where('hotness').between([1, 10]);
const hotOrJen = d.combine.or([hot, jen]);
debugger;
const builder = d.combine.and([hotOrJen, adult]);
console.log(JSON.stringify(builder.getParams(), null, 2));
//# sourceMappingURL=test.js.map