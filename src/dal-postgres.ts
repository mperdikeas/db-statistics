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
  Client,
  QueryResult
} from 'pg';

export type PGTableQuery = {
  schema_name: string;
  table_name: string;
}

export type PGColumnQuery = {
  column_name: string;
  data_type: string;
  is_nullable: string;
  character_maximum_length: number;
}


async function openConnection(dbCoords: DBCoordinates): Promise<Connection> {
  const {host, port, dbname:database, username:user, pwd:password} = dbCoords;
  const client = new Client({user, host, database, password, port});
  await client.connect()
  return new PostgresConnection(client);
}


function convertPGTableQuery(x: QueryResult<PGTableQuery>): SchemaTableInfo[] {
  const rv: SchemaTableInfo[] = [];
  if (x.rows !== undefined) {
    x.rows.forEach( (row: PGTableQuery) => {
      rv.push(new SchemaTableInfo(row.schema_name, row.table_name));
    });
    return rv;
  } else
    assert.fail('failed to retrieve any rows at the PostgreSQL schema/tables query');
}

function convertPGColumnQuery(x: QueryResult<PGColumnQuery>): TableColumnInfo[] {
  const rv: TableColumnInfo[] = [];
  if (x.rows !== undefined) {
    x.rows.forEach( (row: PGColumnQuery) => {
      const {column_name, data_type, is_nullable}: {column_name: string; data_type: string; is_nullable: string} = row;
      const is_nullableb = ((is_nullable)=>{
        switch (is_nullable) {
          case 'YES':
            return true;
          case 'NO':
            return false;
          default:
            assert.fail(`unhandled is_nullable case: [${is_nullable}] for PostgreSQL column [${column_name}]`);
        }
      })(is_nullable);
                            
      rv.push(new TableColumnInfo(column_name, data_type, is_nullableb));
    });
    return rv;
  } else
    assert.fail('failed to retrieve any rows at the PostgreSQL table/columns query');  
}

export class PostgresConnection implements Connection {

  client: Client
  constructor(client: Client) {
    this.client = client;
  }

  async getSchemaTables(schemas: string[]): Promise<SchemaTableInfo[]> {
    const list_of_schemasV: string = list_of_schemas(schemas);
    //@ts-ignore
    const query_1 = `SELECT DISTINCT table_name
                     FROM information_schema.tables
                     WHERE table_schema IN (${list_of_schemasV}) ORDER BY table_name`;

    // https://stackoverflow.com/a/58244497/274677
    const query_2 = `WITH 
                     partition_parents AS (
                         SELECT
                         relnamespace::regnamespace::text AS schema_name,
                         relname                          AS table_name
                         FROM pg_class
                         WHERE relkind = 'p'), -- The parent table is relkind 'p', the partitions are regular tables, relkind 'r'
                     unpartitioned_tables AS (     
                         SELECT
                         relnamespace::regnamespace::text AS schema_name,
                         relname                          AS table_name
                         FROM pg_class
                         WHERE relkind = 'r'
                         AND NOT relispartition
                     ) -- Regular table
                     SELECT * FROM partition_parents 
                     WHERE schema_name in (${list_of_schemasV})
                     UNION
                     SELECT * FROM unpartitioned_tables
                     WHERE schema_name in (${list_of_schemasV})
                     order by 1,2`;

    const rv: QueryResult<PGTableQuery> = await this.client.query(query_2);
    return convertPGTableQuery(rv);
  }

  async close(): Promise<void> {
    await this.client.end();
  }

  async getTableColumns(schema: string, table: string): Promise<TableColumnInfo[]> {
const query = `SELECT
                   column_name,
                   data_type,
                   is_nullable,
                   character_maximum_length 
               FROM
                   information_schema.columns
               WHERE
                   table_schema = '${schema}'
               AND table_name = '${table}'
               ORDER BY ordinal_position`;
    const rv: QueryResult<PGColumnQuery> = await this.client.query(query);
    return convertPGColumnQuery(rv);
  }


  async getNumOfRows(schema: string, table: string): Promise<number> {
    const query = `SELECT COUNT(*) AS n FROM ${schema}.${table}`;
    const rv: QueryResult<{n: string}> = await this.client.query(query);
    if (false)
      console.log(`number of PostgreSQL rows in table ${schema}.${table} is:`, rv.rows);
    if (rv.rows) {
      assert.equal(rv.rows.length, 1, 'fubar #1');
      return parseInt(rv.rows[0].n);
    } else
      assert.fail('fubar #2');
  }

  async getConstraints(schema: string, table: string): Promise<Constraint[]> {
    const query = `
SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
    INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
    INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
WHERE nsp.nspname = '${schema}'
  AND rel.relname = '${table}'
AND con.contype IN ('u', 'p', 'f')`;
    const rv: QueryResult<{conname: string, contype: string}> = await this.client.query(query);
    return rv.rows.map( x=>({name: x.conname, ctype: pg_ctype_to_constraint_type(x.contype)}) );
  }

}

function pg_ctype_to_constraint_type(c: string): ConstraintType {
  switch (c) {
    case 'u':
      return ConstraintType.UNIQUE;
    case 'p':
      return ConstraintType.PRIMARY;
    case 'f':
      return ConstraintType.FOREIGN_KEY;
    default:
      assert.fail(`fubar #3 - can't handle: [${c}]`);
  }
}

export const POSTGRES_DB: DB = {
  openConnection
}


