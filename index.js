const AWS = require('aws-sdk');
const fs = require('fs');

// Configurações
const DYNAMODB_TABLE = 'exemple';
const DYNAMODB_REGION = 'sa-east-1';
const FILE_PATH = `${DYNAMODB_TABLE}.json`;
const FILE_PATH_ITERATOR = `${DYNAMODB_TABLE}-iterator.id`;

AWS.config.update({
  region: DYNAMODB_REGION
});

const dynamodb = new AWS.DynamoDB.DocumentClient();

/**
 * Função para transformar o registro vindo do DynamoDB,
 * para um registro válido do DynamoDB
 *
 * @param object data Registro do DynamoDB
 * @returns string
 */
function transform(data) {
  data._id = data.id;
  delete data.id;

  // Não remover
  return JSON.stringify(data);
}

/**
 * Função utilizada para salvar os registros obtidos,
 * pode ser usada para enviar para o MongoDB, ou 
 * simplesmente salvar em aruqivo como está sendo feito
 * 
 * @param AWS.DynamoDB.DocumentClient.ScanOutput result Resultado da consulta
 * @returns Promise
 */
async function data_save(result) {
  const line = result.Items
    .map(item => transform(item))
    .join("\n");

  return new Promise((resolve, reject) => {
    fs.appendFile(FILE_PATH, line, 'utf8', err => {
      if (err) {
        // Erro ao salvar o registro
        reject(err);
      } else if (result.LastEvaluatedKey) {
        // Registros salvos, continua o processo
        fs.writeFileSync(FILE_PATH_ITERATOR, result.LastEvaluatedKey.id);
        resolve(result.LastEvaluatedKey.id);
      } else {
        // Registros salvos, não há outros registros, termina o processo
        if (fs.existsSync(FILE_PATH_ITERATOR))
          fs.unlinkSync(FILE_PATH_ITERATOR);
        resolve(null);
      }
    });
  });
}

/**
 * Realiza a consulta e obtem os proximos registros
 * 
 * @param string LastEvaluatedKey ID do ultimo registro do DynamoDB
 */
async function get_next(LastEvaluatedKey) {
  const params = {
    TableName: DYNAMODB_TABLE
  };

  if (LastEvaluatedKey) {
    params.ExclusiveStartKey = { id: LastEvaluatedKey };
  }

  const result = await dynamodb.scan(params).promise();
  console.log(`${result.Items.length} items processed`);

  return data_save(result);
}

/**
 * Função responsavel por obter continuamente os
 * dados do DynamoDB
 * 
 * @param {*} next_id 
 */
async function iterator(next_id) {
  try {
    next_id = await get_next(next_id);
    if (next_id) {
      return iterator(next_id);
    } else {
      console.log('Process concluded');
      process.exit(0);
    }
  } catch (e) {
    if (e.code === 'ResourceNotFoundException') {
      console.error(`Table "${DYNAMODB_TABLE}" does not exist in the "${DYNAMODB_REGION}" region`);
      process.exit(0);
    } else {
      console.error(e);
      return iterator(next_id);
    }
  }
}

// Tenta continuar o processo do ultimo ponto salvo
const next_id = fs.existsSync(FILE_PATH_ITERATOR) ? fs.readFileSync(FILE_PATH_ITERATOR).toString() : null;

if (next_id) {
  console.log('Continuing processing');
} else {
  console.log('Starting processing');
}

iterator(next_id);
