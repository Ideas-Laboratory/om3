//@ts-nocheck
import Axios from "axios";
import store from "@/store";

const TOTAL_LEVELS = 19;

export class WaveletTree {
    /**
     * @param {WaveletTree} parent
     * @param {boolean} leftChild
     */
    constructor(parent = null, leftChild = true, index = 0, denoise = { open: false, threshold: 0, method: 'simple_hard' }) {
        this.denoise = denoise;
        this.index = index;
        this.x = 0.5;
        this.width = 1;
        this.yArray = [0, 0, 0, 0]; // Time-min, Time-max, Value-min, Value-max
        this.difference = null;
        this.denoiseYArray = [0, 0, 0, 0];
        this.level = 0;
        this.parent = parent;
        this.position = 0;
        this.fetcher = null;
        this._leftChild = null;
        this._rightChild = null;
        this._listeners = [];
        this._lock = false;
        if (parent) {
            this.width = parent.width / 2;
            this.level = parent.level + 1;
            this.fetcher = parent.fetcher;
            if (leftChild) {
                this.x = parent.x - this.width / 2;
                this.parent._leftChild = this;
                this.position = this.parent.position * 2;
            } else {
                this.x = parent.x + this.width / 2;
                this.parent._rightChild = this;
                this.position = this.parent.position * 2 + 1;
            }
        } else {
            this.fetcher = new TreeNodeFetcher(this.index);
        }
        if (this.level >= TOTAL_LEVELS - 1) {
            throw new Error(
                "This level is protected, please check the TOTAL_LEVELS configuration."
            );
        }
    }

    get isLeafNode() {
        return this.level >= TOTAL_LEVELS - 2;
    }

    get levelNums() {
        return Math.pow(2, this.level);
    }

    get data() {
        if (!this.denoise.open) {
            if (this.level < TOTAL_LEVELS - 2) {
                return this.yArray
                    .map((y, i) => ({
                        x:
                            this.x +
                            this.width *
                            (i < 2
                                ? 1 / 2 - 1 / Math.pow(2, TOTAL_LEVELS - this.level - 1)
                                : 1 / 1024) *
                            Math.pow(-1, i + 1),
                        y,
                    }))
                    .sort((a, b) => a.x - b.x);
            } else {
                return [
                    { x: this.x - this.width / 4, y: this.yArray[0] },
                    { x: this.x + this.width / 4, y: this.yArray[1] },
                ];
            }
        } else {

            if (this.level < TOTAL_LEVELS - 2) {
                return this.denoiseYArray
                    .map((y, i) => ({
                        x:
                            this.x +
                            this.width *
                            (i < 2
                                ? 1 / 2 - 1 / Math.pow(2, TOTAL_LEVELS - this.level - 1)
                                : 1 / 1024) *
                            Math.pow(-1, i + 1),
                        y,
                    }))
                    .sort((a, b) => a.x - b.x);
            } else {
                return [
                    { x: this.x - this.width / 4, y: this.denoiseYArray[0] },
                    { x: this.x + this.width / 4, y: this.denoiseYArray[1] },
                ];
            }
        }
    }

    /**
     * @returns {WaveletTree}
     */
    get leftChild() {
        if (this._leftChild) return this._leftChild;
        try {
            const child = new WaveletTree(this, true, this.index, this.denoise);
            if (!this.difference) {
                child.yArray = this.yArray.map((y, i) =>
                    i == 0
                        ? y
                        : i == 1
                            ? this.yArray[2]
                            : (this.yArray[0] + this.yArray[2]) / 2
                );
                child.denoiseYArray = this.denoiseYArray.map((y, i) =>
                    i == 0
                        ? y
                        : i == 1
                            ? this.denoiseYArray[2]
                            : (this.denoiseYArray[0] + this.denoiseYArray[2]) / 2
                );
                this.fetcher.fetch(child);
            }
            return child;
        } catch (e) {
            return null;
        }
    }

    /**
     * @returns {WaveletTree}
     */
    get rightChild() {
        if (this._rightChild) return this._rightChild;
        try {
            const child = new WaveletTree(this, false, this.index, this.denoise);
            if (!this.difference) {
                child.yArray = this.yArray.map((y, i) =>
                    i == 1
                        ? y
                        : i == 0
                            ? this.yArray[3]
                            : (this.yArray[1] + this.yArray[3]) / 2
                );
                child.denoiseYArray = this.denoiseYArray.map((y, i) =>
                    i == 1
                        ? y
                        : i == 0
                            ? this.denoiseYArray[3]
                            : (this.denoiseYArray[1] + this.denoiseYArray[3]) / 2
                );
                this.fetcher.fetch(child);
            }
            return child;
        } catch (e) {
            return null;
        }
    }

    /**
     * @returns {WaveletTree}
     */
    get previousSibling() {
        return (
            this.parent &&
            (this.parent._leftChild === this
                ? this.parent.previousSibling && this.parent.previousSibling.rightChild
                : this.parent.leftChild)
        );
    }

    /**
     * @returns {WaveletTree}
     */
    get nextSibling() {
        return (
            this.parent &&
            (this.parent._rightChild === this
                ? this.parent.nextSibling && this.parent.nextSibling.leftChild
                : this.parent.rightChild)
        );
    }

    /**
     *
     * @param {[number, number]} range
     * @param {number} detailLevel
     * @param {Function} callback
     */
    getDetailData(range, detailLevel, callback) {
        if (
            range[0] >= this.x + this.width / 2 ||
            range[1] <= this.x - this.width / 2
        ) {
            callback([]);
            return;
        }
        if (this.width > detailLevel && !this.isLeafNode) {
            let leftChildData = null;
            let rightChildData = null;
            this.leftChild.getDetailData(range, detailLevel, (dataLeft) => {
                leftChildData = dataLeft;
                if (leftChildData && rightChildData) {
                    callback(leftChildData.concat(rightChildData));
                }
            });
            this.rightChild.getDetailData(range, detailLevel, (dataRight) => {
                rightChildData = dataRight;
                if (leftChildData && rightChildData) {
                    callback(leftChildData.concat(rightChildData));
                }
            });
            return;
        }
        this._listeners.push(callback);
        callback(this.data);
    }

    notifyUpdate() {
        if (this._lock) return;
        this._lock = true;
        if ((this._leftChild || this._rightChild) && this.difference) {
            const [timeMinDiff, valueMinDiff, valueMaxDiff, timeMaxDiff] =
                this.difference;
            const leftChild = this.leftChild;
            const rightChild = this.rightChild;
            leftChild.yArray = this.yArray.map((v, i) => {
                switch (i) {
                    case 0:
                        return v;
                    case 2:
                        return valueMinDiff < 0 ? v : v + valueMinDiff;
                    case 3:
                        return valueMaxDiff < 0 ? v + valueMaxDiff : v;
                    case 1:
                        return v + timeMaxDiff;
                }
            });
            rightChild.yArray = this.yArray.map((v, i) => {
                switch (i) {
                    case 0:
                        return v - timeMinDiff;
                    case 2:
                        return valueMinDiff < 0 ? v - valueMinDiff : v;
                    case 3:
                        return valueMaxDiff < 0 ? v : v - valueMaxDiff;
                    case 1:
                        return v;
                }
            });
            const denoiseThreshold = this.denoise.threshold;
            if (this.denoise.method === 'simple_hard') {
                leftChild.denoiseYArray = this.yArray.map((v, i) => {
                    switch (i) {
                        case 0:
                            return v;
                        case 2:
                            return valueMinDiff < 0 ? v : v + (Math.abs(valueMinDiff) > denoiseThreshold ? valueMinDiff : 0);

                        case 3:
                            return valueMaxDiff < 0 ? v + (Math.abs(valueMaxDiff) > denoiseThreshold ? valueMaxDiff : 0) : v;

                        case 1:
                            return v + (Math.abs(timeMaxDiff) > denoiseThreshold ? timeMaxDiff : 0);

                    }
                });

                rightChild.denoiseYArray = this.yArray.map((v, i) => {
                    switch (i) {
                        case 0:
                            return v - (Math.abs(timeMinDiff) > denoiseThreshold ? timeMinDiff : 0);
                        case 2:
                            return valueMinDiff < 0 ? v - (Math.abs(valueMinDiff) > denoiseThreshold ? valueMinDiff : 0) : v;
                        case 3:
                            return valueMaxDiff < 0 ? v : v - (Math.abs(valueMaxDiff) > denoiseThreshold ? valueMaxDiff : 0);
                        case 1:
                            return v;
                    }
                });

            } else if (this.denoise.method === 'm4_hard') {
                const leftAvg = leftChild.yArray.reduce((pre, cur) => {
                    return pre + cur;
                }, 0) / 4;

                leftChild.denoiseYArray = leftChild.yArray.map((v, i) => {
                    if (Math.abs(v - leftAvg) <= denoiseThreshold) {
                        return leftAvg;
                    } else {
                        return v;
                    }
                });

                const rightAvg = rightChild.yArray.reduce((pre, cur) => {
                    return pre + cur;
                }, 0) / 4;

                rightChild.denoiseYArray = rightChild.yArray.map((v, i) => {
                    if (Math.abs(v - rightAvg) <= denoiseThreshold) {
                        return rightAvg;
                    } else {
                        return v;
                    }
                });
            } else if (this.denoise.method === 'm4_hard_v2') {
                const leftAvg = leftChild.yArray.reduce((pre, cur) => {
                    return pre + cur;
                }, 0) / 4;
                const rightAvg = rightChild.yArray.reduce((pre, cur) => {
                    return pre + cur;
                }, 0) / 4;

                const allAvg = (leftAvg + rightAvg) / 2;

                leftChild.denoiseYArray = leftChild.yArray.map((v, i) => {
                    if (Math.abs(v - allAvg) <= denoiseThreshold) {
                        return leftAvg;
                    } else {
                        return v;
                    }
                });
                rightChild.denoiseYArray = rightChild.yArray.map((v, i) => {
                    if (Math.abs(v - allAvg) <= denoiseThreshold) {
                        return rightAvg;
                    } else {
                        return v;
                    }
                });
            } else if (this.denoise.method === 'm4_threshold_with_level') {
                const tempThreshold = denoiseThreshold * (TOTAL_LEVELS - leftChild.level) / TOTAL_LEVELS;
                const leftAvg = leftChild.yArray.reduce((pre, cur) => {
                    return pre + cur;
                }, 0) / 4;

                leftChild.denoiseYArray = leftChild.yArray.map((v, i) => {
                    if (Math.abs(v - leftAvg) <= tempThreshold) {
                        return leftAvg;
                    } else {
                        return v;
                    }
                });

                const rightAvg = rightChild.yArray.reduce((pre, cur) => {
                    return pre + cur;
                }, 0) / 4;

                rightChild.denoiseYArray = rightChild.yArray.map((v, i) => {
                    if (Math.abs(v - rightAvg) <= tempThreshold) {
                        return rightAvg;
                    } else {
                        return v;
                    }
                });
            } else if (this.denoise.method === 'm4_wavelet_hard') {
                const tempThreshold = denoiseThreshold * (TOTAL_LEVELS - leftChild.level) / TOTAL_LEVELS;
                const leftDenoiseYArray = this.yArray.map((v, i) => {
                    switch (i) {
                        case 0:
                            return v;
                        case 2:
                            return valueMinDiff < 0 ? v : v + (Math.abs(valueMinDiff) > tempThreshold ? valueMinDiff : 0);

                        case 3:
                            return valueMaxDiff < 0 ? v + (Math.abs(valueMaxDiff) > tempThreshold ? valueMaxDiff : 0) : v;

                        case 1:
                            return v + (Math.abs(timeMaxDiff) > tempThreshold ? timeMaxDiff : 0);

                    }
                });

                const rightDenoiseYArray = this.yArray.map((v, i) => {
                    switch (i) {
                        case 0:
                            return v - (Math.abs(timeMinDiff) > tempThreshold ? timeMinDiff : 0);
                        case 2:
                            return valueMinDiff < 0 ? v - (Math.abs(valueMinDiff) > tempThreshold ? valueMinDiff : 0) : v;
                        case 3:
                            return valueMaxDiff < 0 ? v : v - (Math.abs(valueMaxDiff) > tempThreshold ? valueMaxDiff : 0);
                        case 1:
                            return v;
                    }
                });
                const leftAvg = leftDenoiseYArray.reduce((pre, cur) => {
                    return pre + cur;
                }, 0) / 4;

                leftChild.denoiseYArray = leftDenoiseYArray.map((v, i) => {
                    if (Math.abs(v - leftAvg) <= tempThreshold) {
                        return leftAvg;
                    } else {
                        return v;
                    }
                });

                const rightAvg = rightDenoiseYArray.reduce((pre, cur) => {
                    return pre + cur;
                }, 0) / 4;

                rightChild.denoiseYArray = rightDenoiseYArray.map((v, i) => {
                    if (Math.abs(v - rightAvg) <= tempThreshold) {
                        return rightAvg;
                    } else {
                        return v;
                    }
                });
            }


            leftChild.notifyUpdate();
            rightChild.notifyUpdate();
        }
        this._listeners.forEach((listener) => listener(this.data));
        this._lock = false;
    }

    clearListeners(recursive = false) {
        this._listeners = [];
        if (recursive) {
            this._leftChild && this._leftChild.clearListeners(recursive);
            this._rightChild && this._rightChild.clearListeners(recursive);
        }
    }
}

export class TreeNodeFetcher {
    constructor(index = 0) {
        this.timer = null;
        /**
         * @type {WaveletTree[]}
         */
        this.queue = [];
        this.index = index;
    }

    /**
     * @param {WaveletTree} node
     */
    fetch(node) {
        if (this.queue.includes(node)) return;
        this.queue.push(node);
        if (this.timer) clearTimeout(this.timer);
        this.timer = setTimeout(() => this.fetchFinal(), 10);
    }

    fetchFinal() {
        const pendingList = {};
        const queue = this.queue;
        this.queue = [];
        queue.sort((a, b) => a.position - b.position);
        queue.forEach((node) => {
            if (!pendingList[node.level]) {
                pendingList[node.level] = [];
            }
            let lastRange = pendingList[node.level].pop();
            if (!lastRange) {
                lastRange = [node.position, node.position];
                pendingList[node.level].push(lastRange);
            } else if (node.position === lastRange[1] + 1) {
                lastRange[1] = node.position;
                pendingList[node.level].push(lastRange);
            } else {
                pendingList[node.level].push(lastRange);
                lastRange = [node.position, node.position];
                pendingList[node.level].push(lastRange);
            }
        });
        Object.entries(pendingList).forEach(([level, ranges]) => {
            ranges.forEach((range) => {
                const parentStart = Math.floor(range[0] / 2);
                const parentEnd = Math.floor(range[1] / 2);
                Axios.get(
                    `${store.state.controlParams.currentDB==='DuckDB'?'duckdb':'postgres'}/line_chart/wavelet_progressive_bench?table_name=${store.state.controlParams.currentTable
                    }&width=${(parentEnd - parentStart + 1) * 2
                    }&end=${parentEnd}&current_level=${level - 1}&offset=${parentEnd - parentStart + 1
                    }&start=${parentStart}`
                ).then(({ data }) => {
                    if (data) {
                        const startPosition = data[0][1];
                        for (let i = 0; i < data[1].length; i++) {
                            const nodeLeft = queue.find(
                                (node) =>
                                    node.level === parseInt(level, 10) &&
                                    node.position === startPosition + i * 2
                            );
                            const nodeRight = queue.find(
                                (node) =>
                                    node.level === parseInt(level, 10) &&
                                    node.position === startPosition + i * 2 + 1
                            );
                            if (!nodeLeft && !nodeRight) {
                                return;
                            }
                            const nodeParent = (nodeLeft || nodeRight).parent;
                            nodeParent.difference = [
                                data[1][i],
                                data[2][i],
                                data[3][i],
                                data[4][i],
                            ];
                            nodeParent.notifyUpdate();
                        }
                    }
                });
            });
        });
    }
}
