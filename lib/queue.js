'use strict';
const fs = require('fs');
/**
 * Created by Adrian on 23-Jun-16.
 */
module.exports = (thorin, opt) => {
  const logger = thorin.logger(opt.logger),
    store = Symbol("store"),
    defaultChannel = opt.channel;
  const VIRTUAL_QUEUE = {};

  class Queue {
    constructor() {
      this.connected = false;
      this.virtual = false;
    }

    /* Called when the thorin app is running. */
    run(done) {
      drain();
      if (!opt.store) {  // working with virtual queue.
        this.virtual = true;
        return done();
      }
      let storeObj = thorin.store(opt.store);
      if (!storeObj) {
        logger.error(`Store ${opt.store} is not loaded yet. Working with in-memory queue`);
        this.virtual = true;
        return done();
      }
      if (storeObj.type !== 'redis') {
        logger.warn(`Store ${opt.store} type ${storeObj.type} is not yet supported.`);
        this.virtual = true;
        return done();
      }
      this[store] = storeObj;
      this.connected = storeObj.isConnected();
      storeObj.on('connect', () => {
        this.connected = true;
        drain();
        stopPersist();
      }).on('disconnect', () => {
        this.connected = false;
        startPersist();
      });
      done();
    }

    /**
     * Add an item to the queue.
     * Arguments:
     *  - item -> any item that can be JSON.stringified. When working with in-memory, we will not stringify it.
     *  - channel (optional) -> a separate channel to enqueue to
     *  - fn (optional) -> an optional callback function to be called on enqueue
     * */
    enqueue(item, _channel, _fn) {
      let channel = (typeof _channel === 'string' && _channel ? _channel : defaultChannel),
        fn = (typeof _channel === 'function' ? _channel : (typeof _fn === 'function' ? _fn : false));
      try {
        item = JSON.stringify(item);
      } catch (e) {
        logger.warn(`Failed to stringify enqueued item:`, item);
        return fn && fn(e);
      }
      // Virtual enqueue
      if (this.virtual) {
        if (typeof VIRTUAL_QUEUE[channel] === 'undefined') VIRTUAL_QUEUE[channel] = [];
        VIRTUAL_QUEUE[channel].push(item);
        persist();
        fn && fn();
        return this;
      }
      /* If we're using a store but we're not connected, we persist it. */
      if (!this.connected) {
        if (typeof VIRTUAL_QUEUE[channel] === 'undefined') VIRTUAL_QUEUE[channel] = [];
        VIRTUAL_QUEUE[channel].push(item);
        fn && fn();
        return this;
      }
      // enqueue to redis.
      this[store].exec('RPUSH', channel, item, (err) => {
        if (err) {
          return fn && fn(err);
        }
        return fn && fn();
      });
    }

    /**
     * Dequeues the given number of items from the queue.
     * By default, we dequeue a single item.
     * Arguments:
     *  count -> the number of items to dequeue, default 1
     *  channel (optional) -> a separate channel to dequeue from
     *  fn (optional) -> a callback function to call with the result. If not specified, we will return a promise.
     *
     *    NOTE:
     *      - the result will be an array of items.
     * */
    dequeue(count, _channel, _fn) {
      let channel = (typeof _channel === 'string' && _channel ? _channel : defaultChannel),
        fn = (typeof count === 'function' ? count : (typeof _channel === 'function' ? _channel : (typeof _fn === 'function' ? _fn : false)));
      count = (typeof count === 'number' ? Math.max(1, count) : 1);
      if (!fn) {
        return new Promise((resolve, reject) => {
          doDequeue.call(this, count, channel, (err, res) => {
            if (err) return reject(err);
            resolve(res);
          });
        });
      }
      doDequeue.call(this, count, channel, fn);
      return this;
    }
  }

  function doDequeue(count, channel, fn) {
    if (this.virtual || !this.connected) {
      if (typeof VIRTUAL_QUEUE[channel] === 'undefined') return fn(null, []);
      let items = VIRTUAL_QUEUE[channel].splice(0, count);
      if (VIRTUAL_QUEUE[channel].length === 0) {
        delete VIRTUAL_QUEUE[channel];
      }
      if(this.virtual) {
        persist();
      }
      return fn(null, items);
    }
    let tObj = this[store].multi();
    tObj.exec('LRANGE', channel, 0, count - 1);
    tObj.exec('LTRIM', channel, count, -1);
    tObj.commit((err, res) => {
      if (err) return fn(err);
      let items = res[0];
      for (let i = 0, len = items.length; i < len; i++) {
        try {
          items[i] = JSON.parse(items[i]);
        } catch (e) {
          logger.warn(`Could not parse previously enqueued item ${items[i]} in channel ${channel}`);
        }
      }
      fn(null, items);
    });
  }

  /* Manually drain the persisted queue. This is done synchronously. */
  function drain() {
    if (!opt.logFile) return;  //nothing to drain.
    let cachedData;
    // Read cached data
    try {
      cachedData = fs.readFileSync(opt.logFile, { encoding: 'utf8' });
    } catch(err) {
      logger.error(`Could not read persisted queue for draining`, err);
      return;
    }
    if(cachedData.trim() === '') return;  // nothing to drain.
    // Parse cached data
    try {
      cachedData = JSON.parse(cachedData);
    } catch(e) {
      logger.error(`Could not parse persisted queue for draining`, e);
      return;
    }
    // Clean cached data.
    try {
      fs.writeFileSync(opt.logFile, "", { encoding: 'utf8' });
    } catch(e) {
      logger.warn(`Could not clean persisted queue from draining`, e);
    }
    // For each item in the cached data, re-enqueue it.
    let channels = Object.keys(cachedData),
      enqueued = 0;
    for(let i=0, len = channels.length; i < len; i++) {
      let channel = channels[i];
      for(let j=0, llen = cachedData[channel].length; j < llen; j++) {
        let item = cachedData[channel][j];
        try {
          item = JSON.parse(item);
        } catch(e) {}
        queueObj.enqueue(item, channel);
        enqueued++;
      }
    }
    if(enqueued.length === 0) return;
    logger.trace(`${enqueued} item(s) re-enqueued on drain.`);
  }

  /* Manually persist all the virtual queue to the log file */
  let isPersisting = false;
  function persist() {
    if (!opt.logFile) return;  // not enabled.
    if (isPersisting) return;
    let cacheData;
    try {
      cacheData = JSON.stringify(VIRTUAL_QUEUE);
    } catch (e) {
      logger.warn(`Could not stringify virtual queue for persistance`, e);
      return;
    }
    if(cacheData === "{}") cacheData = "";
    isPersisting = true;
    fs.writeFile(opt.logFile, cacheData, { encoding: 'utf8' }, (err) => {
      if(err) {
        logger.warn(`Could not finalize queue persistance`, err);
      }
      isPersisting = false;
    });
  };

  let persistor = null; // the interval.
  /* Starts the persistor to periodically flush the virtual queue to the log */
  function startPersist() {
    if (!opt.logFile) return;
    let ms = Math.max(opt.logPersist, 500);
    clearInterval(persistor);
    persistor = setInterval(persist, ms);
  }

  /* Stops the persistor supervisor from writing anything to the disk anymore. */
  function stopPersist() {
    if (!opt.logFile) return;
    clearInterval(persistor);
    persistor = null;
  }

  let queueObj = new Queue();

  return queueObj;
};