import { strict as assert } from 'assert';
import {
  DBCoordinates,
  DB,
  Connection,
  SchemaTableInfo,
  TableColumnInfo,
  ConstraintType,
  Constraint
} from './dal-api';

import {
  list_of_schemas
} from './util';

import {
  Result,
  Connection as OracleConnectionImpl
} from 'oracledb';
const oracledb = require('oracledb');

type BOOLEAN = 'Y' | 'N';

type ORATableQueryRow = [string, string]
type ORAColumnQuery = [string, string, number, BOOLEAN]

async function openConnection(dbCoords: DBCoordinates): Promise<Connection> {

  const {host, port, dbname, username, pwd} = dbCoords;
  const ora_conn: OracleConnectionImpl = await oracledb.getConnection({
    user          : username,
    password      : pwd,
    connectString : `(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = ${host})(PORT = ${port}))(CONNECT_DATA =(SID= ${dbname})))`
  });
  return new OracleConnection(ora_conn);
}


function convertORATableQuery(x: Result<ORATableQueryRow>): SchemaTableInfo[] {
  const rv: SchemaTableInfo[] = [];
  if (x.rows !== undefined) {
    x.rows.forEach( (row: ORATableQueryRow) => {
      rv.push(new SchemaTableInfo(row[0], row[1]));
    });
    return rv;
  } else
    assert.fail('failed to retrieve any rows at the Oracle schema/tables query');
}

function convertORAColumnQuery(x: Result<ORAColumnQuery>): TableColumnInfo[] {
  const rv: TableColumnInfo[] = [];
  if (x.rows !== undefined) {
    x.rows.forEach( (row: ORAColumnQuery) => {
      const [column_name, data_type, _, is_nullable]: [string, string, number, string] = row;
      rv.push(new TableColumnInfo(column_name, data_type, is_nullable=='Y'?true:false));
    });
    return rv;
  } else {
    assert.fail('failed to retrieve any rows at the Oracle table/columns query');
  }
}


class OracleConnection implements Connection {

  oraConn: OracleConnectionImpl
  constructor(oraConn: OracleConnectionImpl) {
    this.oraConn = oraConn;
  }

  async getSchemaTables(schemas: string[]): Promise<SchemaTableInfo[]> {
    const list_of_schemasV = list_of_schemas(schemas);
    const ora_tables: Result<ORATableQueryRow> = await this.oraConn.execute(
      `SELECT DISTINCT OWNER, OBJECT_NAME 
       FROM DBA_OBJECTS
       WHERE OBJECT_TYPE = 'TABLE'
       AND OWNER IN (${list_of_schemasV}) ORDER BY OWNER, OBJECT_NAME`);
    return convertORATableQuery(ora_tables);
  }

  async getTableColumns(schema: string, table: string): Promise<TableColumnInfo[]> {
const query = `SELECT col.column_name,
                      col.data_type,
                      col.data_length,
                      col.nullable
               FROM       sys.all_tab_columns col
               INNER JOIN sys.all_tables        t 
               ON                               t.owner      = col.owner
               AND                              t.table_name = col.table_name
               WHERE col.owner      = '${schema}'
               AND   col.table_name = '${table}'
               ORDER BY col.column_id`;
    const rv: Result<ORAColumnQuery> = await this.oraConn.execute(query);
    return convertORAColumnQuery(rv);
  }

  async getNumOfRows(schema: string, table: string): Promise<number> {
    const query = `SELECT COUNT(*) FROM ${schema}.${table}`;
    const rv: Result<[number]> = await this.oraConn.execute(query);
    if (false)
      console.log(`number of Oracle rows in table ${schema}.${table} is: `, rv.rows);
    if (rv.rows) {
      assert.equal(rv.rows.length, 1, `fubar #1`);
      assert.equal(rv.rows[0].length, 1, `fubar #2`);
      return rv.rows[0][0];
    } else {
      assert.fail('fubar #3');
    }
  }


  async getConstraints(schema: string, table: string): Promise<Constraint[]> {
    const query = `
SELECT constraint_name, constraint_type
  FROM all_constraints
 WHERE owner='${schema}'
 AND table_name = '${table}'
 AND constraint_type IN ('R', 'P', 'U')
`;
    const rv: Result<[string, string, string]> = await this.oraConn.execute(query);
    if (rv.rows)
      return rv.rows.map(x=>({name: x[0], ctype: ora_ctype_to_constraint_type(x[1])}));
    else
      assert.fail('fubar #4');
   
  }
  
  async close(): Promise<void> {
    await this.oraConn.close();
  }

}

function ora_ctype_to_constraint_type(c: string): ConstraintType {
  switch (c) {
    case 'U':
      return ConstraintType.UNIQUE;
    case 'P':
      return ConstraintType.PRIMARY;
    case 'R':
      return ConstraintType.FOREIGN_KEY;
    default:
      assert.fail(`fubar #3 - can't handle: [${c}]`);
  }
}

export const ORACLE_DB: DB = {
  openConnection
}
