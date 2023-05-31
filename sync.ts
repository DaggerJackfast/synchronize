import { randexp } from 'randexp';
import * as dotenv from 'dotenv';
import { Collection, MongoClient, ObjectId, Document, FindCursor, UpdateOneModel } from 'mongodb';
import { ChangeStreamEvent, CollectionChangeWatchStream, ICustomer, ICustomerDocument } from './types';

const STRING_RULE_REGEX = /[a-zA-Z\d]{8}/;
const generateRandomString = (): string => {
  return randexp(STRING_RULE_REGEX);
};

const deepCopy = <T>(obj: T): T => {
  if (obj === null || obj === undefined) {
    throw new Error('Object can not be null or undefined when making deep copy');
  }
  return JSON.parse(JSON.stringify(obj));
};

const anonymize = (customer: ICustomer): ICustomer => {
  return {
    firstName: generateRandomString(),
    lastName: generateRandomString(),
    email: anonymizeEmail(customer.email),
    address: {
      line1: generateRandomString(),
      line2: generateRandomString(),
      postcode: generateRandomString(),
      city: customer.address.city,
      country: customer.address.country,
      state: customer.address.state,
    },
    createdAt: customer.createdAt,
  };
};

const anonymizeEmail = (email: string): string => {
  const domain = email.split('@').pop();
  return [generateRandomString(), domain].join('@');
};

type UpdateOneOperation<T extends Document = Document> = {
  updateOne: UpdateOneModel<T>;
};

class CustomersAnonymizer {
  private readonly maxWriteBatchSize: number = 100000;
  private readonly batchCount: number = 1000;
  private readonly saveIntervalTime: number = 1000;
  private readonly client: MongoClient;
  private customerDocuments: ICustomerDocument[] = [];
  private timer: number | NodeJS.Timer | null = null;
  private readonly anonymisedCollection: Collection<ICustomerDocument>;
  private readonly customersCollection: Collection<ICustomerDocument>;

  constructor(client: MongoClient) {
    this.client = client;
    this.anonymisedCollection = this.client.db('synchronize').collection<ICustomerDocument>('customers_anonymised');
    this.customersCollection = this.client.db('synchronize').collection<ICustomerDocument>('customers');
  }

  public async start(fullReindex?: boolean): Promise<void> {
    if (fullReindex) {
      await this.fullReindex();
      process.exit(0);
    }
    await this.watch();
    this.savePreviousCustomers();
  }

  private async fullReindex(): Promise<void> {
    const customerCursor: FindCursor<ICustomerDocument> = await this.customersCollection.find();
    await this.saveWithCursor(customerCursor);
  }

  private async savePreviousCustomers(): Promise<void> {
    const last = await this.anonymisedCollection
      .find({}, { sort: [['createdAt', -1]], projection: { createdAt: 1 }, limit: 1 })
      .next();
    if (!last) {
      return;
    }
    const { createdAt } = last;
    const customerCursor = await this.customersCollection.find({ createdAt: { $gte: createdAt } });
    await this.saveWithCursor(customerCursor);
  }

  private async saveWithCursor(customerCursor: FindCursor<ICustomerDocument>): Promise<void> {
    let updateDocuments: ICustomerDocument[] = [];
    while (await customerCursor.hasNext()) {
      const document = (await customerCursor.next()) as ICustomerDocument;
      updateDocuments.push(document);
      if (updateDocuments.length >= this.maxWriteBatchSize) {
        await this.save(updateDocuments);
        updateDocuments = [];
      }
    }
    if (updateDocuments.length > 0) {
      await this.save(updateDocuments);
    }
  }

  private async watch(): Promise<void> {
    try {
      this.startIntervalSave();
      const pipeline = [
        {
          $match: {
            operationType: { $in: ['update', 'insert'] },
          },
        },
      ];
      const changeStream: CollectionChangeWatchStream<ICustomerDocument> = this.customersCollection.watch(pipeline, {
        fullDocument: 'updateLookup',
      });

      changeStream.on('change', (event: ChangeStreamEvent<ICustomerDocument>): void => {
        if (!event.fullDocument) {
          console.log('document does not exists');
          return;
        }
        const document: ICustomerDocument = event.fullDocument;
        this.customerDocuments.push(document);
        if (this.customerDocuments.length >= this.batchCount) {
          this.save(deepCopy<ICustomerDocument[]>(this.customerDocuments));
          this.customerDocuments = [];
        }
      });
    } catch (error: unknown) {
      console.error(error);
      this.stopIntervalSave();
      throw error;
    }
  }

  private startIntervalSave(): void {
    this.timer = setInterval(() => {
      this.save(deepCopy<ICustomerDocument[]>(this.customerDocuments));
      this.customerDocuments = [];
    }, this.saveIntervalTime);
  }

  private stopIntervalSave(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private createCustomer(document: ICustomerDocument): ICustomer {
    return {
      firstName: document.firstName,
      lastName: document.lastName,
      email: document.email,
      address: {
        line1: document.address.line1,
        line2: document.address.line2,
        postcode: document.address.postcode,
        city: document.address.city,
        state: document.address.state,
        country: document.address.country,
      },
      createdAt: new Date(document.createdAt),
    };
  }

  public async save(documents: ICustomerDocument[]): Promise<void> {
    if (documents.length > 0) {
      console.log('save count: ', documents.length);
      const bulkOperations: UpdateOneOperation<ICustomerDocument>[] = documents.map(document => {
        const customer = this.createCustomer(document);
        const anonymized = anonymize(customer);
        const bulkDocument: ICustomerDocument = { ...anonymized, _id: new ObjectId(document._id) };
        return {
          updateOne: {
            filter: { _id: bulkDocument._id },
            update: {
              $set: { ...bulkDocument },
            },
            upsert: true,
          },
        };
      });
      await this.anonymisedCollection.bulkWrite(bulkOperations);
    }
  }
}

const connect = async (uri: string): Promise<MongoClient> => {
  const client = new MongoClient(uri);
  await client.connect();
  return client;
};

const main = async (): Promise<void> => {
  let client: MongoClient;
  try {
    const fullReindex = process.argv.includes('--full-reindex');
    dotenv.config();
    const uri = <string>process.env.DB_URI;
    client = await connect(uri);
    const watcher = new CustomersAnonymizer(client);
    await watcher.start(fullReindex);

    console.log('after start watching');
  } catch (error) {
    console.error(error);
    throw error;
  }
};
main().then();
