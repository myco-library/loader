const Fs = require('fs');
const Readline = require('readline');
const Gun = require('gun');
require('gun/lib/then.js')

class ol_loader {
    root;
    limit = 10;

    constructor(root, limit) {
        console.info('ol_loader: constructor - initializing');
        this.gun = root;
        this.limit = limit || 10;
    }

    worker (line) {
        const entry = line.split('\t');
        for (let i = entry.length - 1; i >= 0; i--) {
            try {
                const data = JSON.parse(entry[i]);
                switch(data.type.key) {
                    case '/type/author':
                        let key = data.key.split('/authors/').pop();
                        return this.gun.get('Author').get(key).put({name: data.name}).then(() => {
                            console.info(key);
                        });
                    default:
                        throw('ol type "' + data.type.key + '" not recognized.');
                }
            } catch (err) {
                console.error('ol_loader: worker - ' + err + ' ' + entry[i]);
            }
        }
        return Promise.resolve();
    }

    factory (files) {
        console.info('ol_loader: factory - initializing file stream');
        files.forEach(element => {
            try {
                console.info('ol_loader: factory - processing file ' + element)
                let workers = [];
                let lineBuffer = [];
                const rl = Readline.createInterface({
                    input: Fs.createReadStream(element),
                    crlfDelay: Infinity
                }).on('line', (line) => {
                    lineBuffer.push(line);
                    if (lineBuffer.length >= (this.limit * 10)) {
                        rl.pause();
                    }
                }).on('pause', async () => {
                    while (lineBuffer.length > 0) {
                        while ((workers.length < this.limit || lineBuffer.length < this.limit) && lineBuffer.length != 0) {
                            workers.push(this.worker(lineBuffer.pop()));
                        }
                        await Promise.allSettled(workers).finally(() => {
                            workers = [];
                        });
                    }
                    process.nextTick(() => {
                        rl.resume();
                    });
                });
            } catch (err) {
                console.error('ol_loader: factory - ' + err);
            }
        });
    }
}

;(function(){
    const limit = 10;

    const gun = Gun({
        peers: process.env.PEERS && process.env.PEERS.split(',') || [],
        axe: false
    });

    const loader = new ol_loader(
        gun,
        limit
    );

    loader.factory(['ol_dump_authors_2021-10-13.txt']);

	module.exports = [loader, gun];
}());