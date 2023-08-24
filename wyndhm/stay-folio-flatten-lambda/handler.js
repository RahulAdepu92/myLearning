'use strict';
var aws = require('aws-sdk');
const fs = require('fs')
const StaytableDefs = require('./StaytableDefs');
const FoliotableDefs = require('./FoliotableDefs');
const parseString = require('xml2js');
const {v4 : uuidv4} = require('uuid');
// const jpath = require('jspath');

const parser = new parseString.Parser({
  explicitArray: true
});
const s3 = new aws.S3({
  region: 'us-west-2'
});
const sns = new aws.SNS({
  region: 'us-west-2'
});

// Validation Flag
var valid_flag = true;
// Enviorment Variables

const blueBucket = process.env.blueBucket;
const errorBucket = process.env.errorBucket;
const goldBucket = process.env.goldBucket;
const SNSTopicArn = process.env.SNSTopicArn;

// Array Falttening Function
function flatten(ary) {
  var ret = [];
  for (var i = 0; i < ary.length; i++) {
    if (Array.isArray(ary[i])) {
      ret = ret.concat(flatten(ary[i]));
    } else {
      ret.push(ary[i]);
    }
  }
  return ret;
}

// XML to JSON Parser
async function createJSON(xml_buff) {

  try {
    var string_xml=xml_buff.toString();
    string_xml=string_xml.replace(/&/g, '&amp;');
    string_xml=string_xml.replace(/&amp;amp;/g, '&amp;');
    let data = await parser.parseStringPromise(string_xml);

    return data;

  } catch(e) {
    valid_flag = false;
    console.log(`ERROR while parsing the XML. Details - ${String(e)}`);
  }

}

// Function to combine all table data
async function createUberObj(xml_buff,s3outputPath,tableDefs) {
  try {
    let json_ = await createJSON(xml_buff);


    let uberObj = tableDefs.map(table => {

      return {
        tableName: table.tableName,
        records: flatten(table.transform(json_,s3outputPath))
      }

    })

    return uberObj;

  } catch (e) {
    console.log(e)
  }

}

// Generate Timestamp
function create_ts(){

  return (new Date().toISOString())
  
}
// Function to add 90 to UNIX Timestamp and convert into UNIX Epoch Timestamp 
function addDays(date, days) {
  var result = new Date(date);
  result.setDate(result.getDate() + days);
  var expry_dt = Math.floor(+new Date(result) / 1000)
  return expry_dt;
}

// Function to generate UUID
function uuidGen() {
  const uniqueId = uuidv4()
  console.log(uniqueId);
  return uniqueId;
}

function generateESLogs(src_sys,dmn,prc_arn,task_nm,sts,srcName,targetName,errDesc,srcTS,org_fl_nm,prcStartTS,src_cnt,suc_cnt,err_cnt){
    var logs_json_ = { 
      "source_system" : `${src_sys}`, "domain" : `${dmn}`, "subdomain" : null, "process_name" : "Flattening XML to JSON", "process_arn" : `${prc_arn}`, 
        "task_name" : `${task_nm}`, "rule_id" : null, "rule_name" : null, "status" : `${sts}`, "source_name" : `${srcName}`, "target_name" : `${targetName}`,             
        "unique_id" : null, "unique_value_1" : null, "unique_value_2" : null, "unique_value_3" : null, "error_desc" : `${errDesc}`, "design_pattern" : "Realtime",
        "source_system_timestamp" : `${srcTS}`,"business_date" : null, "original_file_name" : `${org_fl_nm}`,"process_start_datetime" : `${prcStartTS}`, 
        "process_end_datetime" : `${create_ts()}`,"batch_id" : null,"source_record_count" : src_cnt, "target_success_count" : suc_cnt, "target_failed_count" : err_cnt};

    console.log(JSON.stringify(logs_json_));

}
// Main (Index) Function
module.exports.index = async (event,context) => {

  var date = new Date();
  var year = String(date.getUTCFullYear());
  var month = ('0' + String(date.getUTCMonth() + 1)).slice(-2);
  var day = ('0' + String(date.getUTCDate())).slice(-2);

  console.log(`EVENT: ${JSON.stringify(event)}`);

  
  let record = event.Records[0];
  let srcTS = record['eventTime']
  let key = decodeURIComponent(record.s3.object.key);
  let key_split = key.split('/')
  let fileName = key_split[key_split.length - 1]

  var S3_Ts = '.' + String(date.getUTCFullYear() +
  '-' + ('0' + String(date.getUTCMonth() + 1)).slice(-2) +
  '-' + ('0' + String(date.getUTCDate())).slice(-2) + 
  '.' + ('0' + String(date.getUTCHours())).slice(-2) +
  ':' + ('0' + String(date.getUTCMinutes())).slice(-2) +
  ':' + ('0' + String(date.getUTCSeconds())).slice(-2) +
  '.' + String((date.getUTCMilliseconds() / 1000).toFixed(3)).slice(2, 5));
  
  var fileName_Ts = fileName+S3_Ts

  valid_flag =  true;
  
  let s3_get_paras = {
    Bucket: blueBucket,
    Key: key,
  }

  let resp = await s3.getObject(s3_get_paras).promise();
  let payload = resp.Body

 

  try {

    if(key.includes('pms_stay')){
      var s3_path = `s3://${blueBucket}/pms_stay/intermediate/${year}/${month}/${day}/${fileName_Ts}.json`;

      var s3_key = `pms_stay/intermediate/${year}/${month}/${day}/${fileName_Ts}.json`;

      var s3_error_malformed_key = `pms_stay/errors/malformed/${year}/${month}/${day}/${fileName_Ts}`;
      var s3_error_generic_key = `pms_stay/errors/generic/${year}/${month}/${day}/${fileName_Ts}`;
      var MissingStay_filepath = `pms_stay/stay_transaction_table/${year}/${month}/${day}/${fileName}.txt`;
      
      // Stay Files for HTCH Bacth Watch
      var s3_gold_htcs_paras ={
        Key: `htcs/pms_stay/${fileName.slice(2,7)}/${year}/${month}/${fileName}`,
        Bucket: goldBucket,
        Body: payload,
        ServerSideEncryption: "AES256"
      };

      await s3.putObject(s3_gold_htcs_paras).promise();
      console.log(`SUCCESS Putting Stay File to Gold Terminal S3 for HTCS Batch Watch: ${s3_gold_htcs_paras['Key']}`);

      // Stay Files for Amperity
      var s3_gold_amperity_paras ={
        Key: `pms-stay/${year}/${month}/${day}/${fileName.substring(0,2).toUpperCase()}/xml/${fileName}`,
        Bucket: goldBucket,
        Body: payload,
        ServerSideEncryption: "AES256"
      };

      await s3.putObject(s3_gold_amperity_paras).promise();
      console.log(`SUCCESS Putting Stay File to Gold Terminal S3 for Amperity: ${s3_gold_amperity_paras['Key']}`);

      
      var output = await createUberObj(payload,s3_path, StaytableDefs);
      if(valid_flag === true){
        var jso2 = {};
        for(let y of output){
          jso2[`${y.tableName}`] = y.records
        };
        var ID = uuidGen();
        var BrandID = fileName.substring(0,2).toUpperCase();
        var SiteID = fileName.substring(2,7);
        var BusinessDate = jso2['stg_btch_hdr'][0]['dt_thru'];
        var RecivedDate = `${year}${month}${day}`;

        let MissingStay_str = `ID,BrandID,SiteID,BusinessDate,RecivedDate` +'\n'+ `${ID},${BrandID},${SiteID},${BusinessDate},${RecivedDate}`;
         console.log(MissingStay_str)

        var s3_MissingStay_data ={
            Key: MissingStay_filepath,
            Bucket: blueBucket,
            Body: MissingStay_str,
            ServerSideEncryption: "AES256"
        };
        await s3.putObject(s3_MissingStay_data).promise();
        console.log(`Putting Stay Transaction File on S3: ${MissingStay_filepath}`);

      }

      
    }else{
      var s3_path = `s3://${blueBucket}/pms_folio/intermediate/${year}/${month}/${day}/${fileName_Ts}.json`;

      var s3_key = `pms_folio/intermediate/${year}/${month}/${day}/${fileName_Ts}.json`;
      
      var s3_error_malformed_key = `pms_folio/errors/malformed/${year}/${month}/${day}/${fileName_Ts}`;
      var s3_error_generic_key = `pms_folio/errors/generic/${year}/${month}/${day}/${fileName_Ts}`;

      var output = await createUberObj(payload,s3_path, FoliotableDefs);
    }

    if(valid_flag === true){
        var jso1 = {};

        for (let x of output) {
          jso1[`${x.tableName}`] = x.records
        };
        // console.log(JSON.stringify(jso1))
        
        //S3 Blue Paras
        var s3_paras = {
          Key: s3_key,
          Bucket: blueBucket,
          Body: JSON.stringify(jso1),
          ServerSideEncryption: "AES256"
        };

        await s3.putObject(s3_paras).promise();
        console.log(` successfully processed`);

        if(key.includes('pms_stay')){
          generateESLogs('PMS Stay','Stay',context.invokedFunctionArn,'Flattening XML to JSON','SUCCESS',`s3://${blueBucket}/${key}`,`${s3_path}`,null,srcTS,`s3://${blueBucket}/${key}`,create_ts(),1,1,0);
        }else{
          generateESLogs('PMS Folio','Folio',context.invokedFunctionArn,'Flattening XML to JSON','SUCCESS',`s3://${blueBucket}/${key}`,`${s3_path}`,null,srcTS,`s3://${blueBucket}/${key}`,create_ts(),1,1,0);
        }

    }else{
      console.log(`File is Malformed XML`);

      // Putting Malformed XML File to Error Bucket
      
      var s3_error_paras = {
          Key: s3_error_malformed_key,
          Bucket: errorBucket,
          Body: payload,
          ServerSideEncryption: "AES256"
      }

      await s3.putObject(s3_error_paras).promise();
      console.log(`Putting Malformed XML at S3 path: s3://${errorBucket}/${s3_error_paras['Key']}`);

      if(key.includes('pms_stay')){
        generateESLogs('PMS Stay','Stay',context.invokedFunctionArn,'Flattening XML to JSON','ERROR',`s3://${blueBucket}/${key}`,`s3://${errorBucket}/${s3_error_malformed_key}`,'Malformed File',srcTS,`s3://${blueBucket}/${key}`,create_ts(),1,0,1);
      }else{
        generateESLogs('PMS Folio','Folio',context.invokedFunctionArn,'Flattening XML to JSON','ERROR',`s3://${blueBucket}/${key}`,`s3://${errorBucket}/${s3_error_malformed_key}`,'Malformed File',srcTS,`s3://${blueBucket}/${key}`,create_ts(),1,0,1);
      }

    }
    
  } catch (e) {
    console.log(`ERROR: ${String(e)}`);

    var s3_error_paras = {
      Key: s3_error_generic_key,
      Bucket: errorBucket,
      Body: payload,
      ServerSideEncryption: "AES256"
    }

    await s3.putObject(s3_error_paras).promise();

    if(key.includes('pms_stay')){
      generateESLogs('PMS Stay','Stay',context.invokedFunctionArn,'Flattening XML to JSON','ERROR',`s3://${blueBucket}/${key}`,`s3://${errorBucket}/${s3_error_generic_key}`,`${String(e)}`,srcTS,`s3://${blueBucket}/${key}`,create_ts(),1,0,1);
    }else{
      generateESLogs('PMS Folio','Folio',context.invokedFunctionArn,'Flattening XML to JSON','ERROR',`s3://${blueBucket}/${key}`,`s3://${errorBucket}/${s3_error_generic_key}`,`${String(e)}`,srcTS,`s3://${blueBucket}/${key}`,create_ts(),1,0,1);
    }

    const sns_paras = {
      TopicArn: SNSTopicArn,
      Subject: 'Stay and Folio Files to JSON Transform Lambda Failed',
      Message: `ERROR Details: ${String(e)}`
    };
    await sns.publish(sns_paras).promise();

  };
};