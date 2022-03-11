export type DBCoordinates = {
  host: string;
  port: number;
  dbname: string;
  username: string;
  pwd: string
}

export interface DB {
  openConnection(dbCoors: DBCoordinates): Promise<Connection>;
}

export class SchemaTableInfo {
  schema: string;
  table: string;

  constructor(schema: string, table: string) {
    this.schema = schema;
    this.table = table;
  }

  normalize(): SchemaTableInfo {
    return new SchemaTableInfo(this.schema.toUpperCase(), this.table.toUpperCase());
  }

}

export class TableColumnInfo {
  name: string;
  data_type: string;
  is_nullable: boolean;
  constructor(name: string,
              data_type: string,
              is_nullable: boolean) {
    this.name = name;
    this.data_type = data_type;
    this.is_nullable = is_nullable;
  }
}


export enum ConstraintType {
  PRIMARY, UNIQUE, FOREIGN_KEY
}
  

export type Constraint = {
  name: string;
  ctype: ConstraintType
}
  

export interface Connection {

  getSchemaTables(schemas: string[]): Promise<SchemaTableInfo[]>;
  getTableColumns(schema: string, table: string): Promise<TableColumnInfo[]>;
  getNumOfRows(schema: string, table: string): Promise<number>;
  getConstraints(schema: string, table: string): Promise<Constraint[]>;
  close(): Promise<void>;

}
