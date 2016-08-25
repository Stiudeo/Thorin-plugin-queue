'use strict';
const path = require('path'),
  initQueue = require('./lib/queue.js');

module.exports = function(thorin, opt, pluginName) {
  const defaultOpt= {
    logger: pluginName || 'queue',
    store: null,          // the redis store to use for enqueue/dequeueing. If none is given, we work with an in-memory queue.
    channel: 'thorin.queue',    // the default channel to enqueue to /dequeue from
    logFile: 'config/.queue',  // if specified, this will be the file that we're going to use to write queue logs.
    logPersist: 1000        // number of milliseconds between log persisting. This not to lose any enqueues
  };
  opt = thorin.util.extend(defaultOpt, opt);
  if(opt.logFile) {
    opt.logFile = path.normalize(path.isAbsolute(opt.logFile) ? opt.logFile : thorin.root + '/' + opt.logFile);
  }
  function noop(){};
  const queueObj = initQueue(thorin, opt),
    QUEUE_CACHE = {},
    logger = thorin.logger(opt.logger);
  QUEUE_CACHE[opt.channel] = queueObj;
  /* Create or get a queue */
  queueObj.get = function(queueOpt) {
    queueOpt = thorin.util.extend(opt, queueOpt || {});
    if(typeof QUEUE_CACHE[queueOpt.channel] === 'undefined') {
      queueObj.create(queueOpt);
    }
    return QUEUE_CACHE[queueOpt.channel];
  };

  /*
  * Manually a new queue object.
  * */
  queueObj.create = function (queueOpt) {
    queueOpt = thorin.util.extend(opt, queueOpt || {});
    if(queueOpt.logFile) {
      queueOpt.logFile = path.normalize(path.isAbsolute(queueOpt.logFile) ? queueOpt.logFile : thorin.root + '/' + queueOpt.logFile);
    }
    if(typeof QUEUE_CACHE[queueOpt.channel] !== 'undefined') return QUEUE_CACHE[queueOpt.channel];
    let newQueue = initQueue(thorin, queueOpt, true);
    newQueue.run(noop);
    QUEUE_CACHE[queueOpt.channel] = newQueue;
    return newQueue;
  }
  /*
  * Setup the queue plugin
  * */
  queueObj.setup = function(done) {
    if(!opt.logFile) return done();
    try {
      thorin.util.fs.ensureFileSync(opt.logFile);
    } catch(e) {
      logger.warn(`Could not create log file ${opt.logFile}`);
      return done(e);
    }
    thorin.addIgnore(path.basename(opt.logFile));
    done();
  }
  return queueObj;
};
module.exports.publicName = 'queue';