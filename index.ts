import { SchemaRegistry } from './schema-registry';
import {
  destinationPassword,
  destinationUrl,
  destinationUsername,
  sourcePassword,
  sourceUrl,
  sourceUsername
} from './config';

const productionSchemaRegistry = new SchemaRegistry({
  url: sourceUrl,
  username: sourceUsername,
  password: sourcePassword,
  env: 'prod'
});

const localSchemaRegistry = new SchemaRegistry({
  url: destinationUrl,
  username: destinationUsername,
  password: destinationPassword,
  env: 'local'
});

const handleSubjectVersion = async (subject: string, version: number) => {
  const schema = await productionSchemaRegistry.getSubjectVersionById(subject, version);

  if (schema.references) {
    for await (const item of schema.references) {
      await handleSubjectVersion(item.subject, item.version);
    }
  }

  const exists = await localSchemaRegistry.isSchemaExists(subject, version);

  if (exists) {
    console.log(`Subject ${subject} with the version ${version} is already created`);
    return;
  }

  await localSchemaRegistry.createSchema(schema);
}

const handleSubject = async (subject: string) => {
  const versions = await productionSchemaRegistry.getSubjectVersions(subject);

  for await (const item of versions) {
    await handleSubjectVersion(subject, item);
  }
}

const run = async () => {
  await localSchemaRegistry.setImportMode();

  const subjects = await productionSchemaRegistry.getSubjects();

  for await (const item of subjects) {
    await handleSubject(item);
  }

  await localSchemaRegistry.setReadWriteMode();
}

run();
