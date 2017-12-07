"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.batchProcess = ({ client, builder, worker }) => __awaiter(this, void 0, void 0, function* () {
    let lastEvaluatedKey = null;
    let retry = true;
    let keepGoing = true;
    let response;
    const params = builder.getParams();
    while (keepGoing && (lastEvaluatedKey || retry)) {
        try {
            response = yield client[builder.type](params).promise();
        }
        catch (err) {
            if (err.retryable) {
                retry = true;
                yield waitImmediate();
                continue;
            }
            retry = false;
            throw err;
        }
        retry = false;
        lastEvaluatedKey = response.LastEvaluatedKey;
        if (lastEvaluatedKey) {
            params.ExclusiveStartKey = lastEvaluatedKey;
        }
        else {
            delete params.ExclusiveStartKey;
        }
        keepGoing = yield worker(response);
    }
});
exports.loadAll = ({ builder, client }) => __awaiter(this, void 0, void 0, function* () {
    const responses = [];
    yield exports.batchProcess({
        builder,
        client,
        worker: batch => {
            responses.push(batch);
        }
    });
    return exports.mergeResults(responses, builder.schema.TableName);
});
exports.mergeResults = (responses, tableName) => {
    const result = {
        Items: [],
        ConsumedCapacity: {
            CapacityUnits: 0,
            TableName: tableName
        },
        Count: 0,
        ScannedCount: 0
    };
    const merged = responses.reduce((memo, resp) => {
        if (!resp) {
            return memo;
        }
        memo.Count += resp.Count || 0;
        memo.ScannedCount += resp.ScannedCount || 0;
        if (resp.ConsumedCapacity) {
            memo.ConsumedCapacity.CapacityUnits += resp.ConsumedCapacity.CapacityUnits || 0;
        }
        if (resp.Items) {
            memo.Items = memo.Items.concat(resp.Items);
        }
        if (resp.LastEvaluatedKey) {
            memo.LastEvaluatedKey = resp.LastEvaluatedKey;
        }
        return memo;
    }, result);
    if (merged.ConsumedCapacity.CapacityUnits === 0) {
        delete merged.ConsumedCapacity;
    }
    if (merged.ScannedCount === 0) {
        delete merged.ScannedCount;
    }
    return merged;
};
const waitImmediate = () => new Promise(resolve => setImmediate(resolve));
//# sourceMappingURL=exec.js.map