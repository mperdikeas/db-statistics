import { strict as assert } from 'assert';

const COORDINATES_FILENAME      = 'coordinates.json';
const COORDINATES_FPATH         = `./${COORDINATES_FILENAME}`;
const COORDINATES_TEMPLATE_FILE = `${COORDINATES_FILENAME}.TEMPLATE`;
import * as fs from 'fs';
if (!fs.existsSync(COORDINATES_FPATH)) { 
  console.error(`\n\nFile [${COORDINATES_FILENAME}] not found, please create it using file [${COORDINATES_TEMPLATE_FILE}] as template\n\n`);
  process.exit(1);
}


const oracledb = require('oracledb');

import {
  SchemaTableInfo,
  Connection,
  DBCoordinates
} from './dal-api';

import {ORACLE_DB  } from './dal-oracle';



async function reportOnRows(oraConn: Connection, _oraTables: SchemaTableInfo[]) {

  const oraTables: SchemaTableInfo[] = _oraTables.map( (x)=>x.normalize() );
  const ora_num = oraTables.length;
  console.log(`${ora_num} tables found`);
  let total = 0;
  for (const y of oraTables) {
    const on = await oraConn.getNumOfRows(y.schema, y.table);
    total += on;
    console.log(`schema: ${y.schema} | table: ${y.table} | num-of-rows: ${on}`);
  }
  console.log(`A total of ${total} rows in ${ora_num} tables`);
}



async function doWork() {
  let ora_conn: Connection | undefined;
  try {

    const oracle_schemas = ['GAEE2021'];


    oracledb.initOracleClient();
    const ora_coords: DBCoordinates = JSON.parse(fs.readFileSync(COORDINATES_FPATH, 'utf-8'));
    ora_conn = await ORACLE_DB  .openConnection(ora_coords);


    if (ora_conn) {
      const ora_tables: SchemaTableInfo[] = await ora_conn.getSchemaTables(oracle_schemas);
      await reportOnRows(ora_conn, ora_tables);
    } else {
      assert.fail('could not obtain Oracle connection');
    }

  } finally {
    if (ora_conn) {
      await ora_conn.close();
    }
  }
}


doWork();





