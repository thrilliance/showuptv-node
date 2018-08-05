const Promise = require('bluebird');
const fs = Promise.promisifyAll(require('fs'));
const childProcess = require('child_process');
const path = require('path');
const bhttp = require('bhttp');
const colors = require('colors');
const moment = require('moment');
const yaml = require('js-yaml');
const WebSocketClient = require('websocket').client;
const mvAsync = Promise.promisify(require('mv'));
const mkdirpAsync = Promise.promisify(require('mkdirp'));
const Queue = require('promise-queue');

Queue.configure(Promise.Promise);
const queue = new Queue(2, Infinity);

const session = bhttp.session();

const config = yaml.safeLoad(fs.readFileSync('config.yml', 'utf8'));

config.captureDirectory = config.captureDirectory || 'capture';
config.completeDirectory = config.completeDirectory || 'complete';
config.modelScanInterval = config.modelScanInterval || 60;
config.minFileSizeMb = config.minFileSizeMb || 5;
config.debug = !!config.debug;
config.rtmpDebug = !!config.rtmpDebug;
config.models = Array.isArray(config.models) ? config.models : [];
config.dateFormat = config.dateFormat || 'YYYYMMDD-HHmmss';
config.createModelDirectory = !!config.createModelDirectory;

const captureDirectory = path.resolve(__dirname, config.captureDirectory);
const completeDirectory = path.resolve(__dirname, config.completeDirectory);
const minFileSize = config.minFileSizeMb * 1048576;

let captures = [];

function printMsg(...args) {
  console.log.apply(console, [colors.gray(`[${moment().format('MM/DD/YYYY - HH:mm:ss')}]`), ...args]);
}

const printErrorMsg = printMsg.bind(printMsg, colors.red('[ERROR]'));
const printDebugMsg = config.debug ? printMsg.bind(printMsg, colors.yellow('[DEBUG]')) : () => {};

function getRtmpArguments(model) {
  return Promise
    .try(() => session.get(`http://showup.tv/${model}`))
    .then((response) => {
      const rawHTML = response.body.toString('utf8');

      const startChildBug = rawHTML.match(/startChildBug\(user\.uid, '([^']*)', '([^']*)'/);

      if (!startChildBug || !startChildBug[2]) {
        throw new Error('startChildBug is unavailable');
      }

      const csrf = startChildBug[1];
      const wsUrl = startChildBug[2];

      printDebugMsg('csrf:', csrf, 'wsUrl:', wsUrl);

      const user = rawHTML.match(/var transUser = new User\(([\s\S]+?),/);

      if (!user || !user[1]) {
        throw new Error('User\'s uid is unavailable');
      }

      const userUid = user[1];

      printDebugMsg('userUid:', userUid);

      return { csrf, wsUrl, userUid };
    })
    .timeout(10000)
    .then((params) => {
      let webSocketConnection;

      return new Promise((resolve, reject) => {
        const client = new WebSocketClient();
        const rtmpArguments = {};

        client.on('connectFailed', reject);

        client.on('connect', (connection) => {
          webSocketConnection = connection;

          connection.on('error', reject);

          connection.on('message', (message) => {
            if (message.type === 'utf8') {
              const json = JSON.parse(message.utf8Data);

              if (json.id === 102 && json.value[0]) {
                if (json.value[0] === 'failure') {
                  printDebugMsg(colors.green(model), 'might be offline');

                  resolve(null);
                } else if (json.value[0] === 'alreadyJoined') {
                  reject(new Error('Another stream of this model exists'));
                } else {
                  rtmpArguments.streamServer = json.value[1];
                }
              }

              if (json.id === 103 && json.value[0]) {
                rtmpArguments.playpath = `${json.value[0]}_aac`;
              }

              if (json.id === 143 && json.value[0] === '0') {
                // printDebugMsg('Logged in');
              }

              if (rtmpArguments.streamServer && rtmpArguments.playpath) {
                resolve(rtmpArguments);
              }
            }
          });

          connection.sendUTF(`{ "id": 0, "value": [${params.userUid}, "${params.csrf}"]}`);
          connection.sendUTF(`{ "id": 2, "value": ["${model}"]}`);
        });

        client.connect(`ws://${params.wsUrl}`, '');
      })
        .timeout(10000)
        .finally(() => {
          printDebugMsg('Close WebSocket connection');

          if (webSocketConnection) {
            webSocketConnection.close();
          }
        });
    });
}

function captureModel(model) {
  const capture = captures.find(c => c.model === model);

  // this should never happen, but just in case...
  if (capture) {
    printDebugMsg(colors.green(model), 'is already capturing');
    return null; // resolve immediately
  }

  return Promise
    .try(() => getRtmpArguments(model))
    .then((rtmpArguments) => {
      // if rtmpArguments is not set then the model is offline or there weres some issues
      if (rtmpArguments) {
        printMsg(colors.green(model), 'is online, starting rtmpdump process');

        const filename = `${model}_${moment().format(config.dateFormat)}.flv`;

        const spawnArguments = [
          '--live',
          config.rtmpDebug ? '' : '--quiet',
          '--rtmp', `rtmp://${rtmpArguments.streamServer}:1935/webrtc`,
          '--playpath', rtmpArguments.playpath,
          '--flv', path.join(captureDirectory, filename),
        ];

        printDebugMsg('spawnArguments:', spawnArguments);

        const proc = childProcess.spawn('rtmpdump', spawnArguments);

        proc.stdout.on('data', (data) => {
          printMsg(data.toString());
        });

        proc.stderr.on('data', (data) => {
          printMsg(data.toString());
        });

        proc.on('close', () => {
          printMsg(colors.green(model), 'stopped streaming');

          captures = captures.filter(c => c.model !== model);

          const src = path.join(captureDirectory, filename);
          const dst = config.createModelDirectory
            ? path.join(completeDirectory, model, filename)
            : path.join(completeDirectory, filename);

          fs.statAsync(src)
            // if the file is big enough we keep it otherwise we delete it
            .then(stats => (stats.size <= minFileSize
              ? fs.unlinkAsync(src)
              : mvAsync(src, dst, { mkdirp: true })
            ))
            .catch((err) => {
              if (err.code !== 'ENOENT') {
                printErrorMsg(colors.red(`[${model}]`), err.toString());
              }
            });
        });

        if (proc.pid) {
          captures.push({
            model,
            filename,
            proc,
            checkAfter: moment().unix() + 60, // we are gonna check the process after 60 seconds
            size: 0,
          });
        }
      }
    })
    .catch((err) => {
      printErrorMsg(colors.red(`[${model}]`), err.toString());
    });
}

function checkCapture(capture) {
  if (!capture.checkAfter || capture.checkAfter > moment().unix()) {
    // if this is not the time to check the process then we resolve immediately
    printDebugMsg(colors.green(capture.model), '- OK');
    return null;
  }

  printDebugMsg(colors.green(capture.model), 'should be checked');

  return fs
    .statAsync(path.join(captureDirectory, capture.filename))
    .then((stats) => {
      // we check the process after 60 seconds since the its start,
      // then we check it every 10 minutes,
      // if the size of the file has not changed over the time, we kill the process
      if (stats.size - capture.size > 0) {
        printDebugMsg(colors.green(capture.model), '- OK');

        capture.checkAfter = moment().unix() + 600; // 10 minutes
        capture.size = stats.size;
      } else if (capture.model) {
        // we assume that onClose will do all the cleaning for us
        printErrorMsg(colors.red(`[${capture.model}]`), 'Process is dead');
        capture.childProcess.kill();
      }
    })
    .catch((err) => {
      if (err.code === 'ENOENT') {
        // do nothing, file does not exists,
      } else {
        printErrorMsg(colors.red(`[${capture.model}]`), err.toString());
      }
    });
}

function mainLoop() {
  printDebugMsg('Start searching for new models');

  return Promise
    .try(() => config.models.filter(m => !captures.find(p => p.model === m)))
    .then(modelsToCapture => new Promise((resolve) => {
      printDebugMsg(`${modelsToCapture.length} model(s) to capture`);

      if (modelsToCapture.length === 0) {
        resolve();
      } else {
        modelsToCapture.forEach((m) => {
          queue.add(() => captureModel(m)).then(() => {
            if ((queue.getPendingLength() + queue.getQueueLength()) === 0) {
              resolve();
            }
          });
        });
      }
    }))
    .then(() => Promise.all(captures.map(checkCapture)))
    .catch(printErrorMsg)
    .finally(() => {
      captures.forEach((c) => {
        printDebugMsg(colors.grey(c.proc.pid.toString().padEnd(12, ' ')), colors.grey(c.checkAfter), colors.grey(c.filename));
      });

      printMsg('Done, will search for new models in', config.modelScanInterval, 'second(s).');

      setTimeout(mainLoop, config.modelScanInterval * 1000);
    });
}

Promise
  .try(() => session.get('http://showup.tv/site/accept_rules/yes?ref=http://showup.tv/site/log_in', {
    headers: {
      referer: 'http://showup.tv/site/accept_rules?ref=http://showup.tv/site/log_in',
    },
  }))
  .then(() => mkdirpAsync(captureDirectory))
  .then(() => mkdirpAsync(completeDirectory))
  .then(() => mainLoop())
  .catch((err) => {
    printErrorMsg(err.toString());
  });
