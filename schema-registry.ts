import fetch from 'node-fetch';

interface ISchemaRegistryParams {
  url: string;
  username: string;
  password: string;
  env: string;
}

interface ISchemaRegistryReference {
  name: string;
  subject: string;
  version: number;
}

interface ISchemaRegistrySchema {
  "subject": string;
  "version": number;
  "id": number;
  "schema": string;
  "references"?: ISchemaRegistryReference[],
}

interface ISchemaRegistryError {
  error_code: number;
  message: string;
}

export class SchemaRegistry {
  private readonly url: string;
  private readonly username: string;
  private readonly password: string;
  private readonly env: string;

  constructor(params: ISchemaRegistryParams) {
    this.url = params.url;
    this.username = params.username;
    this.password = params.password;
    this.env = params.env;
  }

  async getSchemas(): Promise<ISchemaRegistrySchema[]> {
    const request = await this.query('/schemas');

    const result = await request.json() as ISchemaRegistrySchema[];

    if (request.status !== 200) {
      console.log(`${this.env} get schema result:`, result);
    }

    return result;
  }

  async getSubjects(): Promise<string[]> {
    const request = await this.query('/subjects');

    const result = await request.json() as string[];

    if (request.status !== 200) {
      console.log(`${this.env} get subjects result:`, result);
    }

    return result;
  }

  async getSubjectVersions(subject: string): Promise<number[]> {
    const request = await this.query(`/subjects/${subject}/versions`);

    const result = await request.json() as number[];

    if (request.status !== 200) {
      console.log(`${this.env} get subject version result:`, result);
    }

    return result;
  }

  async getSubjectVersionById(subject: string, version: number): Promise<ISchemaRegistrySchema> {
    const request = await this.query(`/subjects/${subject}/versions/${version}`);

    const result = await request.json() as ISchemaRegistrySchema;

    if (request.status !== 200) {
      console.log(`${this.env} get subject version by id result:`, result);
    }

    return result;
  }

  async setImportMode() {
    const request = await this.query('/mode', 'PUT', JSON.stringify({
      mode: 'IMPORT'
    }));
    const result = await request.json();

    if (request.status !== 200) {
      console.log(`${this.env} set import mode result:`, result);
    }
  }

  async setReadWriteMode() {
    const request = await this.query('/mode', 'PUT', JSON.stringify({
      mode: 'READWRITE'
    }));
    const result = await request.json();

    if (request.status !== 200) {
      console.log(`${this.env} set import mode result:`, result);
    }
  }

  async isSchemaExists(subject: string, version: number) {
    const result = await this.getSubjectVersionById(subject, version);

    return !('error_code' in result);
  }

  async createSchema(schema: ISchemaRegistrySchema) {
    const request = await this.query(`/subjects/${schema.subject}/versions`, 'POST', JSON.stringify({
      version: schema.version,
      id: schema.id,
      schema: schema.schema,
      references: schema.references
    }), {
      'Content-Type': 'application/vnd.schemaregistry.v1+json'
    });

    const result = await request.json();

    if (request.status !== 200) {
      console.log(`${this.env} create schema result:`, result);
    }

    return result;
  }

  private query(path: string, method: 'GET' | 'POST' | 'PUT' = 'GET', body: any = {}, headers: any = {
    'Content-Type': 'application/json'
  }) {
    const authorization = this.username ? `Basic ${btoa(`${this.username}:${this.password}`)}` : '';

    const options = method === 'GET' ? {
      method,
      headers: {
        Authorization: authorization,
        ...headers
      },
    } : {
      method,
      headers: {
        Authorization: authorization,
        ...headers
      },
      body
    };

    return fetch(`${this.url}${path}`, options)
  }
}
