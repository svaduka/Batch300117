ADD JAR /home/edureka/SAIWS/Dataset/HIVE/myudfs.jar;

DROP DATABASE if exists healthDB cascade;

CREATE DATABASE healthDB;
USE healthDB;

CREATE TABLE healthCareSampleDS (PatientID INT, Name STRING, DOB STRING, PhoneNumber STRING, EmailAddress STRING, SSN STRING, Gender STRING, Disease STRING, weight FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/edureka/SAIWS/Dataset/HIVE/healthcare_Sample_dataset1.csv' INTO table healthCareSampleDS;
LOAD DATA LOCAL INPATH '/home/edureka/SAIWS/Dataset/HIVE/healthcare_Sample_dataset2.csv' INTO table healthCareSampleDS;

CREATE TEMPORARY FUNCTION deIdentify AS 'myudfs.Deidentify';

CREATE TABLE healthCareSampleDSDeidentified AS SELECT PatientID, deIdentify(Name), deIdentify(DOB), deIdentify(PhoneNumber), deIdentify(EmailAddress), deIdentify(SSN), deIdentify(Gender), deIdentify(Disease), deIdentify(weight) FROM healthCareSampleDS;

insert overwrite directory '/user/edureka/hive/output'
select * from healthCareSampleDSDeidentified limit 10;
