"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class ExpressionAttributes {
    constructor() {
        this.addName = (name) => {
            const keys = name.split('.');
            const path = `#${keys.join('.#').replace(/[^\w\.#]/g, '')}`;
            keys.forEach(key => {
                this.names[`#${key.replace(/[^\w\.]/g, '')}`] = key;
            });
            return path;
        };
        this.addValue = (value) => {
            const name = ':v' + this.counter++;
            this.values[name] = value;
            return name;
        };
        this.names = {};
        this.values = {};
        this.counter = 0;
    }
}
exports.ExpressionAttributes = ExpressionAttributes;
//# sourceMappingURL=attributes.js.map