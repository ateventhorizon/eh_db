const mongoose = require("mongoose");
// NDDADO
// WARNING, THIS LINE **WILL** CREATE PROBLEMS, YOU HAVE BEEN WARNED
// globlal.db is used as a poor version of dependency inversion to allow external modules like *auth* to use a single
// database connection (mongoose instance) without that we couldn't have modules that shares the same database instance.
// I reckon we'll need encapsulation of some sort to achieve real dependency inversion, as this global state it's pants.
global.db = mongoose;

const mongodb = require("mongodb");
const delay = require('delay');
const md5 = require("md5");
const streamifier = require('streamifier');
const stream = require('stream');
const util = require('util');
const logger = require("eh_logger");
const globalConfig = require("eh_config");
//Set up default mongoose connection

exports.initDB = async () => {
  const dbMaxConnectionTimeSeconds = 300;
  const dbConnectionRetryInterval = 55;
  let dbConnectionTimeElaped = 0;
  const mongoDBUrl = `mongodb://${globalConfig.MongoDBURI}/${globalConfig.MongoDBdbName}?replicaSet=${globalConfig.MongoDBReplicaSetName}`;

  logger.info(mongoDBUrl);
  while (dbConnectionTimeElaped < dbMaxConnectionTimeSeconds) {
    try {
      await mongoose.connect(mongoDBUrl, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        useFindAndModify: false,
        useCreateIndex:true
      });
      exports.bucketSourceAssets = new mongodb.GridFSBucket(mongoose.connection.client.db(globalConfig.MongoDBdbName), {bucketName: "fs_assets_to_elaborate"});
      exports.bucketEntities = new mongodb.GridFSBucket(mongoose.connection.client.db(globalConfig.MongoDBdbName), {bucketName: "fs_entity_assets"});
      logger.info("MongoDB connected with GridFS");

      return mongoose.connection;
    } catch (err) {
      logger.warn(err);
      await delay(dbConnectionRetryInterval * 1000);
      dbConnectionTimeElaped += dbConnectionRetryInterval;
    }
  }

  return null;
};

const isObjectEmpty = obj => {
  return Object.entries(obj).length === 0 && obj.constructor === Object;
};

let Writable = stream.Writable;
let memStore = {};

/* Writable memory stream */
function WMStrm(key, options) {
  // allow use without new operator
  if (!(this instanceof WMStrm)) {
    return new WMStrm(key, options);
  }
  Writable.call(this, options); // init super
  this.key = key; // save key
  memStore[key] = new Buffer.from(''); // empty
}

util.inherits(WMStrm, Writable);

WMStrm.prototype._write = function (chunk, enc, cb) {
  // our memory store stores things in buffers
  let buffer = (Buffer.isBuffer(chunk)) ?
    chunk :  // already is Buffer use it
    new Buffer.from(chunk, enc);  // string, convert

  // concat to the buffer already there
  memStore[this.key] = Buffer.concat([memStore[this.key], buffer]);
  cb();
};

const downloadByName = async (bucketFSModel, filename, callback) => {
  return new Promise(resolve => {
    let wstream = new WMStrm('file');
    wstream.on('finish', () => {
      callback();
      resolve();
    });
    bucketFSModel.openDownloadStreamByName(filename).pipe(wstream);
  });
};

const downloadById = async (bucketFSModel, id) => {
  let data;
  await new Promise(resolve => {
    let wstream = new WMStrm('file');
    wstream.on('finish', () => {
      data = memStore.file;
      resolve();
    });
    bucketFSModel.openDownloadStream(id).pipe(wstream);
  });
  return data;
};

exports.fsFind = async (bucketFSModel, id) => {
  try {
    const ret = bucketFSModel.find({_id: id});
    const entries = await ret.toArray();
    return entries[0];
  } catch (e) {
    logger.error("fsFind of: " + id + " failed because " + e);
    return null;
  }
};

exports.fsExists = async (bucketFSModel, filename, metadata) => {
  try {
    const ret = bucketFSModel.find({filename: filename, ...metadata});
    const entries = await ret.toArray();
    logger.info("Filename " + filename + (entries.length > 0 ? " already exists" : " not found"));
    return entries;
  } catch (e) {
    logger.error("fsExists of: " + filename + " failed because " + e);
    return null;
  }
};

exports.fsEqual = async (bucketFSModel, filename, data, metadata) => {
  try {
    const queryLength = {filename: filename, ...metadata, length: data.length};
    const existSameLengthAsset = bucketFSModel.find(queryLength);
    let ret = false;
    if (existSameLengthAsset) {
      await downloadByName(bucketFSModel, filename, () => {
        if (md5(memStore.file) === md5(data)) {
          ret = true;
        }
      });
    }
    logger.info("Exact copy of " + filename + (ret ? "" : " NOT") + " found");
    return ret;
  } catch (e) {
    logger.error("fsEqual of: " + filename + " failed because " + e);
    return null;
  }
};

exports.fsDelete = async (bucketFSModel, id) => {
  try {
    logger.info("Deleting asset: " + id.toString());
    await bucketFSModel.delete(id);
  } catch (e) {
    logger.error("fsDelete of: " + id + " failed because " + e);
  }
};

exports.fsInsert = async (bucketFSModel, filename, data, metadata) => {
  try {
    logger.info("Inserting new asset: " + filename);
    return new Promise((resolve, reject) => {
      streamifier.createReadStream(data).pipe(bucketFSModel.openUploadStream(filename, {
        metadata: metadata,
        disableMD5: true
      })).on('error', reject).on('finish', resolve);
    });
  } catch (e) {
    logger.error("fsInsert of: " + filename + " failed because " + e);
  }
};

exports.fsUpsert = async (bucketFSModel, filename, data, metadata, metadataComp, entityCheckFSID = null) => {

  try {
    logger.info("Performing upsert...");
    let bPerformInsert = true;
    const entries = await module.exports.fsExists(bucketFSModel, filename, metadataComp);
    for (const elem of entries) {
      const bIsExactCopy = await module.exports.fsEqual(bucketFSModel, filename, data, metadataComp);
      let bIsUsedByAnEntity = false;
      if (!bIsExactCopy) {
        await module.exports.fsDelete(bucketFSModel, elem._id);
      } else {
        if (entityCheckFSID !== null) {
          logger.info("Check if fsid is used by any entity...");
          bIsUsedByAnEntity = await entityCheckFSID(elem.fsid);
        }
      }
      bPerformInsert &= !bIsExactCopy || !bIsUsedByAnEntity;
    }
    if (bPerformInsert) {
      await module.exports.fsInsert(bucketFSModel, filename, data, metadata);
    } else {
      const msg = "No upsert operation performed of " + filename + " because there's already an exact binary copy present";
      const msgHuman = filename + " already present. \nNo operation done.";
      let json = {msg: "daemonLogger", data: {type: "warning", msg: msgHuman}};
      // socketController.sendMessageToAllClients(JSON.stringify(json));
      logger.info(msg);
    }
    return bPerformInsert;
  } catch (e) {
    logger.error("fsUpsert of: " + filename + " failed because " + e);
    return false;
  }
};

exports.fsDownloadWithId = async (bucketFSModel, id) => {
  try {
    logger.info("Download asset: " + id);
    return await downloadById(bucketFSModel, id);
  } catch (e) {
    logger.error("fsDownloadWithId of: " + id + " failed because " + e);
  }
};

exports.objectId = (objString) => {
  return mongodb.ObjectId(objString);
};

exports.upsert = async ( model, query = {}, data, options = {}) => {
  try {
    const queryFinal = isObjectEmpty(query) ? data : query;
    await model.findOneAndUpdate(queryFinal, data, {new: true,upsert: true, ...options});
    return await model.findOne(query);
  } catch (e) {
    logger.error(e);
    return null;
  }
};

exports.upsertUniqueXValue = async (model, query) => {
  let queryOnly = query;
  const values = query.values;
  delete queryOnly.values;

  const data = {
    ...query,
    $push: {
      values: {
        $each: values,
        $sort: {x: 1}
      }
    }
  };
  const ret = await exports.upsert(model, queryOnly, data);

  let newValues = [];
  for (let index = 0; index < ret.values.length - 1; index++) {
    if (ret.values[index].x !== ret.values[index + 1].x) {
      newValues.push(ret.values[index]);
    }
  }
  newValues.push(ret.values[ret.values.length - 1]);

  await model.updateOne(query, {
    $set: {
      values: newValues,
    }
  });
};
