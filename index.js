"use strict";

var _ = require("underscore"),
    Promise = require("bluebird"),
    aws = require("aws-sdk"),
    knox = require("knox"),
    UploadStream = require('s3-upload-stream');

function S3(config) {
    this._config = config;

    aws.config.update({
        accessKeyId: config.key,
        secretAccessKey: config.secret,
        region: config.region
    });

    this._s3 = new aws.S3();
    this._uploadStream = new UploadStream(this._s3);
    this._knox = knox.createClient(_.pick(this._config, "key", "secret", "bucket"));
}

S3.prototype.initializeBucket = function (options) {
    return this.getBucket(_.pick(options, "Bucket"))
        .catch(function (err) {
            if (err.statusCode === 404) return this.createBucket(options);

            return Promise.reject(err);
        }.bind(this));
};

S3.prototype.getBucket = function (options) {
    var s3 = this._s3;

    return new Promise(function (resolve, reject) {
        s3.headBucket(options, function (err) {
            if (err) return reject(err);

            return resolve();
        });
    });
};

S3.prototype.createBucket = function (options) {
    var s3 = this._s3;

    return new Promise(function (resolve, reject) {
        s3.createBucket(options, function (err) {
            if (err) return reject(err);

            return resolve();
        })
    });
};

S3.prototype.list = function (options) {
    var s3 = this._s3;

    return new Promise(function (resolve, reject) {
        s3.listObjects(options, function (err, items) {
            if (err) return reject(err);

            return resolve(items);
        });
    });
};

S3.prototype.upload = function (stream, options) {
    var config = this._config,
        uploadStream = this._uploadStream;

    return new Promise(function (resolve, reject) {
        var upload = uploadStream.upload(options);

        if (config.concurrentParts) {
            upload.concurrentParts(config.concurrentParts);
        }

        upload.on("error", reject);
        upload.on("uploaded", resolve);

        stream.pipe(upload);
    });
};

S3.prototype.delete = function (filename) {
    var knox = this._knox;

    return new Promise(function (resolve, reject) {
        knox
            .del(filename)
            .on("response", function (res) {
                if (res.statusCode !== 204) return reject(res);

                return resolve();
            })
            .end();
    });
};

S3.prototype.deleteMultiple = function (keys) {
    var knox = this._knox;

    return new Promise(function (resolve, reject) {
        knox.deleteMultiple(keys, function (err, res) {
            if (err) return reject(err);
            if (res.statusCode !== 200) return reject(res);

            return resolve();
        });
    });
};

S3.prototype.download = function (filename) {
    var knox = this._knox;

    return new Promise(function (resolve, reject) {
        knox.getFile(filename, function (err, res) {
            if (err) return reject(err);

            return resolve(res);
        });
    });
};

module.exports = S3;