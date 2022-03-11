export function list_of_schemas(schemas: string[]) {
  return schemas.map( s=>`'${s}'`).join(', ');
}
