CREATE DATABASE IF NOT EXISTS bigdata;
USE bigdata;

CREATE TABLE if not exists `uservisits` (
    `sourceIP` VARCHAR(116),
    `destURL` VARCHAR(300),
    `visitDate` DATE,
    `adRevenue` FLOAT,
    `userAgent` VARCHAR(256),
    `countryCode` CHAR(3),
    `languageCode` CHAR(6),
    `searchWord` VARCHAR(32),
    `duration` INT(11),
    SHARD KEY(destURL),
    KEY(destURL),
    KEY(searchWord)
);

CREATE TABLE if not exists `rankings` (
    `pageURL` VARCHAR(300),
    `pageRank` INT(11),
    `avgDuration` INT(11),
    PRIMARY KEY(pageURL),
    FOREIGN SHARD KEY(pageURL) references uservisits (destURL),
    KEY(pageRank)
);
